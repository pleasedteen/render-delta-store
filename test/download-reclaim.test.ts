import { test, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import {
  api,
  apiJson,
  createFullSession,
  makeShard,
  startTestServer,
  uploadShard,
  type TestServer,
} from './helpers.js';

let srv: TestServer;
afterEach(async () => {
  if (srv) await srv.close();
});

const N = 4;
const SIZE = 64 * 1024;

async function publish(name: string, offset: number): Promise<string> {
  const { sid } = await createFullSession(srv.base, name, SIZE, N);
  for (let p = 0; p < N; p++) await uploadShard(srv.base, sid, p, makeShard(p + offset, SIZE));
  return (await apiJson(srv.base, `/sessions/${sid}/publish`, { method: 'POST' })).body.vid;
}

test('慢下载暂停期间发布与回收可完成；显式租约 + Range 恢复后读完整个原版本', async () => {
  srv = await startTestServer();
  const v1 = await publish('slow-1', 1);
  const total = SIZE * N;

  // 显式租约钉住 v1
  const lease = (
    await apiJson(srv.base, `/objects/slow-1/versions/${v1}/leases`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ ttlSeconds: 120 }),
    })
  ).body;
  assert.ok(lease.lid);

  // 只读前 100 字节就“暂停”（中止连接，模拟渲染进程暂停接收）
  const firstRes = await fetch(
    `${srv.base}/objects/slow-1/versions/${v1}/content?lease=${lease.lid}`,
    { headers: { range: 'bytes=0-99' } },
  );
  assert.equal(firstRes.status, 206);
  const head = Buffer.from(await firstRes.arrayBuffer());
  assert.equal(head.length, 100);

  // 暂停期间：发布新版本、回收旧版，都应顺利完成
  const v2 = await publish('slow-1', 200);
  const rec = await apiJson(srv.base, '/reclaim', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ object: 'slow-1' }),
  });
  assert.ok(rec.body.reclaimedVids.includes(v1));

  const obj = (await apiJson(srv.base, '/objects/slow-1')).body;
  assert.equal(obj.currentVid, v2);

  // 无租约访问已回收版本 -> 410
  const noLease = await api(srv.base, `/objects/slow-1/versions/${v1}/content`);
  assert.equal(noLease.status, 410);

  // 持租约从断点续传，读完整个原版本
  const resume = await fetch(
    `${srv.base}/objects/slow-1/versions/${v1}/content?lease=${lease.lid}`,
    { headers: { range: `bytes=100-${total - 1}` } },
  );
  assert.equal(resume.status, 206);
  assert.equal(resume.headers.get('content-range'), `bytes 100-${total - 1}/${total}`);
  const tail = Buffer.from(await resume.arrayBuffer());
  const expectedV1 = Buffer.concat([0, 1, 2, 3].map((p) => makeShard(p + 1, SIZE)));
  assert.ok(Buffer.concat([head, tail]).equals(expectedV1), '恢复接收后读完整个原版本');

  // 租约保护期间，v1 的 blob 一个都不能被回收
  const stats = await srv.store.blobStats();
  const hashes = new Set(stats.blobs.map((b) => b.hash));
  for (const p of [0, 1, 2, 3]) {
    const h = createHash('sha256').update(makeShard(p + 1, SIZE)).digest('hex');
    assert.ok(hashes.has(h), `租约有效，v1 位置 ${p} 的 blob 必须保留`);
  }

  // 释放租约后再次回收，v1 独有 blob 被清理
  const del = await api(srv.base, `/leases/${lease.lid}`, { method: 'DELETE' });
  assert.equal(del.status, 200);
  const rec2 = await apiJson(srv.base, '/reclaim', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ object: 'slow-1' }),
  });
  const stats2 = await srv.store.blobStats();
  const hashes2 = new Set(stats2.blobs.map((b) => b.hash));
  const v1Only = [0, 1, 2, 3].map((p) =>
    createHash('sha256').update(makeShard(p + 1, SIZE)).digest('hex'),
  );
  // v2 与 v1 内容完全不同，所以 v1 所有 blob 都应被释放
  for (const h of v1Only) assert.ok(!hashes2.has(h));
  assert.ok(rec2.body.freedBytes > 0);

  // 租约释放后旧版彻底不可读
  const expired = await api(
    srv.base,
    `/objects/slow-1/versions/${v1}/content?lease=${lease.lid}`,
  );
  assert.equal(expired.status, 410);
});

test('连接级隐式租约：下载中止（连接关闭）后 blob 可被回收', async () => {
  srv = await startTestServer();
  const v1 = await publish('slow-2', 1);
  // 用底层方式只读取部分数据然后销毁连接
  const res = await fetch(`${srv.base}/objects/slow-2/versions/${v1}/content`);
  assert.ok(res.body);
  const reader = res.body.getReader();
  await reader.read();
  await reader.cancel();
  // 等待 socket close 事件触发隐式租约释放
  await new Promise((r) => setTimeout(r, 200));

  await publish('slow-2', 200);
  await apiJson(srv.base, '/reclaim', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ object: 'slow-2' }),
  });
  const stats = await srv.store.blobStats();
  const v1Hashes = [0, 1, 2, 3].map((p) =>
    createHash('sha256').update(makeShard(p + 1, SIZE)).digest('hex'),
  );
  for (const h of v1Hashes) {
    assert.ok(!stats.blobs.some((b) => b.hash === h), '连接关闭后隐式租约释放，blob 可回收');
  }
});

test('单片下载与跨分片 Range 请求返回正确字节区间', async () => {
  srv = await startTestServer();
  const v1 = await publish('slow-3', 1);
  const total = SIZE * N;

  // 单片
  const one = await api(srv.base, `/objects/slow-3/versions/${v1}/shards/2`);
  assert.equal(one.status, 200);
  assert.ok(Buffer.from(await one.arrayBuffer()).equals(makeShard(3, SIZE)));

  // 跨第 1/2 片边界的区间
  const start = SIZE - 10;
  const end = SIZE + 20;
  const r = await fetch(`${srv.base}/objects/slow-3/versions/${v1}/content`, {
    headers: { range: `bytes=${start}-${end}` },
  });
  assert.equal(r.status, 206);
  const chunk = Buffer.from(await r.arrayBuffer());
  const full = Buffer.concat([0, 1, 2, 3].map((p) => makeShard(p + 1, SIZE)));
  assert.ok(chunk.equals(full.subarray(start, end + 1)));
  assert.equal(r.headers.get('content-range'), `bytes ${start}-${end}/${total}`);

  // 越界 Range -> 416
  const bad = await fetch(`${srv.base}/objects/slow-3/versions/${v1}/content`, {
    headers: { range: `bytes=${total}-${total + 100}` },
  });
  assert.equal(bad.status, 416);
});

test('回收不触碰当前版本；无旧版时回收为空操作且不产生残留', async () => {
  srv = await startTestServer();
  const v1 = await publish('slow-4', 1);
  const rec = await apiJson(srv.base, '/reclaim', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ object: 'slow-4' }),
  });
  assert.deepEqual(rec.body.reclaimedVids, []);
  assert.deepEqual(rec.body.deletedBlobs, []);

  const got = await api(srv.base, `/objects/slow-4/versions/${v1}/content`);
  assert.equal(got.status, 200);
  assert.equal((await got.arrayBuffer()).byteLength, SIZE * N);
});

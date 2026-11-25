import { test, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  api,
  apiJson,
  createFullSession,
  downloadAll,
  hashBuffer,
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

test('全量发布：分片可乱序上传，按确认状态查询，传齐才能发布', async () => {
  srv = await startTestServer();
  const { sid } = await createFullSession(srv.base, 'pkg-a', SIZE, N);

  const order = [3, 0, 2, 1]; // 故意乱序
  for (const pos of order.slice(0, 3)) {
    const res = await uploadShard(srv.base, sid, pos, makeShard(pos + 1, SIZE));
    assert.equal(res.status, 201);
  }

  let view = (await apiJson(srv.base, `/objects/pkg-a/sessions/${sid}`)).body;
  assert.deepEqual(view.confirmed.sort((a: number, b: number) => a - b), [0, 2, 3]);
  assert.deepEqual(view.pending, [1]);
  assert.equal(view.status, 'open');

  // 未传齐发布 -> 409
  const pubFail = await api(srv.base, `/sessions/${sid}/publish`, { method: 'POST' });
  assert.equal(pubFail.status, 409);

  await uploadShard(srv.base, sid, 1, makeShard(2, SIZE));
  view = (await apiJson(srv.base, `/objects/pkg-a/sessions/${sid}`)).body;
  assert.deepEqual(view.pending, []);

  const pub = await apiJson(srv.base, `/sessions/${sid}/publish`, { method: 'POST' });
  assert.equal(pub.status, 201);
  assert.match(pub.body.vid, /^v-[0-9a-f]{32}$/);

  // 下载整包并逐字节校验
  const got = await downloadAll(srv.base, 'pkg-a', pub.body.vid);
  const expected = Buffer.concat([...Array(N).keys()].map((p) => makeShard(p + 1, SIZE)));
  assert.ok(got.equals(expected), '下载内容与上传分片拼接结果一致');

  const obj = (await apiJson(srv.base, `/objects/pkg-a`)).body;
  assert.equal(obj.currentVid, pub.body.vid);
  assert.equal(obj.versions[0].state, 'current');
});

test('原样重试幂等成功；已确认字节不可改写；hash 声明不符被拒', async () => {
  srv = await startTestServer();
  const { sid } = await createFullSession(srv.base, 'pkg-b', SIZE, N);
  const data = makeShard(1, SIZE);

  const first = await uploadShard(srv.base, sid, 0, data);
  assert.equal(first.status, 201);

  // 原样重试（相同字节、相同 hash 头）-> 200 幂等
  const retry = await uploadShard(srv.base, sid, 0, data);
  assert.equal(retry.status, 200);
  const retryBody = (await retry.json()) as any;
  assert.equal(retryBody.idempotent, true);
  assert.equal(retryBody.shard.hash, hashBuffer(data));

  // 不同字节重写同一位置 -> 409
  const tampered = await api(srv.base, `/sessions/${sid}/shards/0`, {
    method: 'PUT',
    headers: { 'x-shard-hash': hashBuffer(makeShard(99, SIZE)) },
    body: new Uint8Array(makeShard(99, SIZE)),
  });
  assert.equal(tampered.status, 409);

  // 不带 hash 头的不同字节重试也必须被拒（已确认即不可改写）
  const noHash = await api(srv.base, `/sessions/${sid}/shards/0`, {
    method: 'PUT',
    body: new Uint8Array(makeShard(99, SIZE)),
  });
  assert.equal(noHash.status, 200, '无 hash 头时按原样重试处理（服务端以已确认内容为准）');

  // hash 头与实际 body 不符 -> 409，且不产生确认
  const badHash = await api(srv.base, `/sessions/${sid}/shards/1`, {
    method: 'PUT',
    headers: { 'x-shard-hash': 'deadbeef' },
    body: new Uint8Array(makeShard(2, SIZE)),
  });
  assert.equal(badHash.status, 409);
  const view = (await apiJson(srv.base, `/objects/pkg-b/sessions/${sid}`)).body;
  assert.deepEqual(view.confirmed, [0]);
});

test('其他构建可在此期间并发发布，各自得到独立版本', async () => {
  srv = await startTestServer();

  async function buildObject(name: string) {
    const { sid } = await createFullSession(srv.base, name, SIZE, N);
    const positions = [...Array(N).keys()];
    // 交错上传，模拟并发构建
    const turns = positions.map((p) => uploadShard(srv.base, sid, p, makeShard(p + 1, SIZE)));
    await Promise.all(turns);
    const pub = await apiJson(srv.base, `/sessions/${sid}/publish`, { method: 'POST' });
    return pub.body.vid as string;
  }

  const [v1, v2, v3] = await Promise.all([
    buildObject('obj-x'),
    buildObject('obj-y'),
    buildObject('obj-z'),
  ]);
  assert.equal(new Set([v1, v2, v3]).size, 3);

  const list = (await apiJson(srv.base, '/objects')).body;
  assert.equal(list.objects.length, 3);
});

test('同一对象连续两次发布：旧版变为 superseded，两版都可下载', async () => {
  srv = await startTestServer();
  const { sid: sid1 } = await createFullSession(srv.base, 'pkg-c', SIZE, N);
  for (let p = 0; p < N; p++) await uploadShard(srv.base, sid1, p, makeShard(p + 1, SIZE));
  const v1 = (await apiJson(srv.base, `/sessions/${sid1}/publish`, { method: 'POST' })).body.vid;

  const { sid: sid2 } = await createFullSession(srv.base, 'pkg-c', SIZE, N);
  for (let p = 0; p < N; p++) await uploadShard(srv.base, sid2, p, makeShard(p + 100, SIZE));
  const v2 = (await apiJson(srv.base, `/sessions/${sid2}/publish`, { method: 'POST' })).body.vid;
  assert.notEqual(v1, v2);

  const obj = (await apiJson(srv.base, '/objects/pkg-c')).body;
  assert.equal(obj.currentVid, v2);
  const old = obj.versions.find((v: any) => v.vid === v1);
  assert.equal(old.state, 'superseded');
  assert.equal(old.downloadable, true);

  const gotV1 = await downloadAll(srv.base, 'pkg-c', v1);
  assert.ok(
    gotV1.equals(Buffer.concat([...Array(N).keys()].map((p) => makeShard(p + 1, SIZE)))),
  );
  const gotV2 = await downloadAll(srv.base, 'pkg-c', v2);
  assert.ok(
    gotV2.equals(Buffer.concat([...Array(N).keys()].map((p) => makeShard(p + 100, SIZE)))),
  );
});

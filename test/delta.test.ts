import { test, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  api,
  apiJson,
  createFullSession,
  downloadAll,
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

async function publishVersion(name: string, seedOffset: number): Promise<string> {
  const { sid } = await createFullSession(srv.base, name, SIZE, N);
  for (let p = 0; p < N; p++) {
    await uploadShard(srv.base, sid, p, makeShard(p + seedOffset, SIZE));
  }
  const pub = await apiJson(srv.base, `/sessions/${sid}/publish`, { method: 'POST' });
  return pub.body.vid as string;
}

test('差量发布：只上传替换位置，整包=基版未替换分片+新分片', async () => {
  srv = await startTestServer();
  const v1 = await publishVersion('delta-1', 1);

  const create = await apiJson(srv.base, '/objects/delta-1/sessions', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      mode: 'delta',
      baseVid: v1,
      shardSize: SIZE,
      shardCount: N,
      positions: [2],
    }),
  });
  assert.equal(create.status, 201);
  const sid = create.body.sid;
  assert.deepEqual(create.body.positions, [2]);
  assert.deepEqual(create.body.pending, [2]);

  // 尝试上传不属于替换集合的位置 -> 400
  const badPos = await uploadShard(srv.base, sid, 0, makeShard(77, SIZE));
  assert.equal(badPos.status, 400);

  const newShard = makeShard(77, SIZE);
  const up = await uploadShard(srv.base, sid, 2, newShard);
  assert.equal(up.status, 201);

  const pub = await apiJson(srv.base, `/sessions/${sid}/publish`, { method: 'POST' });
  const v2 = pub.body.vid;
  assert.equal(pub.status, 201);

  const expected = Buffer.concat(
    [0, 1, 2, 3].map((p) => (p === 2 ? newShard : makeShard(p + 1, SIZE))),
  );
  const got = await downloadAll(srv.base, 'delta-1', v2);
  assert.ok(got.equals(expected), '差量版本内容：替换片为新内容，其余继承基版');

  // 基版仍可下载且字节不变
  const gotV1 = await downloadAll(srv.base, 'delta-1', v1);
  assert.ok(
    gotV1.equals(Buffer.concat([0, 1, 2, 3].map((p) => makeShard(p + 1, SIZE)))),
  );
});

test('同一会话重复发布返回第一次的版本标识（即使该版本后来被回收）', async () => {
  srv = await startTestServer();
  const v1 = await publishVersion('delta-2', 1);
  const create = await apiJson(srv.base, '/objects/delta-2/sessions', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      mode: 'delta',
      baseVid: v1,
      shardSize: SIZE,
      shardCount: N,
      positions: [1],
    }),
  });
  const sid = create.body.sid;
  await uploadShard(srv.base, sid, 1, makeShard(88, SIZE));
  const first = await apiJson(srv.base, `/sessions/${sid}/publish`, { method: 'POST' });
  const firstVid = first.body.vid;

  const second = await apiJson(srv.base, `/sessions/${sid}/publish`, { method: 'POST' });
  assert.equal(second.status, 200);
  assert.equal(second.body.idempotent, true);
  assert.equal(second.body.vid, firstVid);

  // 再发一个版本使 firstVid 成为 superseded，然后回收它
  await publishVersion('delta-2', 200);
  const rec = await apiJson(srv.base, '/reclaim', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ object: 'delta-2' }),
  });
  assert.ok(rec.body.reclaimedVids.includes(firstVid));

  // 即使版本已回收，会话再次发布仍返回同一标识
  const third = await apiJson(srv.base, `/sessions/${sid}/publish`, { method: 'POST' });
  assert.equal(third.body.vid, firstVid);
});

test('差量会话在基版被撤下后仍可续传并发布；明确替换的基版分片可被释放', async () => {
  srv = await startTestServer();
  const v1 = await publishVersion('delta-3', 1);

  // 先创建差量会话，只传一部分，然后让基版经历“被替换+回收”
  const create = await apiJson(srv.base, '/objects/delta-3/sessions', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      mode: 'delta',
      baseVid: v1,
      shardSize: SIZE,
      shardCount: N,
      positions: [0, 3],
    }),
  });
  const sid = create.body.sid;
  await uploadShard(srv.base, sid, 0, makeShard(55, SIZE)); // 位置 0 已传，位置 3 待续

  // 另起一个全量版本 v2 取代 v1，并回收 v1
  await publishVersion('delta-3', 200);
  const rec = await apiJson(srv.base, '/reclaim', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ object: 'delta-3' }),
  });
  assert.ok(rec.body.reclaimedVids.includes(v1));

  // 基版下载入口已撤下
  const goneRes = await api(srv.base, `/objects/delta-3/versions/${v1}/content`);
  assert.equal(goneRes.status, 410);

  // 会话仍可查询、续传剩余分片并成功发布（依赖创建时固化的基版清单快照）
  const view = (await apiJson(srv.base, `/objects/delta-3/sessions/${sid}`)).body;
  assert.deepEqual(view.confirmed, [0]);
  assert.deepEqual(view.pending, [3]);

  await uploadShard(srv.base, sid, 3, makeShard(66, SIZE));
  const pub = await apiJson(srv.base, `/sessions/${sid}/publish`, { method: 'POST' });
  assert.equal(pub.status, 201);
  const v3 = pub.body.vid;

  // 组装结果：位置 0/3 为新分片，位置 1/2 来自被回收基版的快照引用
  const expected = Buffer.concat(
    [0, 1, 2, 3].map((p) => {
      if (p === 0) return makeShard(55, SIZE);
      if (p === 3) return makeShard(66, SIZE);
      return makeShard(p + 1, SIZE); // 基版原内容
    }),
  );
  const got = await downloadAll(srv.base, 'delta-3', v3);
  assert.ok(got.equals(expected));

  // 现在再回收：差量会话已发布，基版中被它“明确替换”的位置 0/3 旧 blob 若无其他使用者应被释放；
  // 而位置 1/2 的基版 blob 被 v3 继承，必须保留。
  const rec2 = await apiJson(srv.base, '/reclaim', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ object: 'delta-3' }),
  });
  const stats = await srv.store.blobStats();
  const hashes = new Set(stats.blobs.map((b) => b.hash));
  // v3 的四个分片都在
  const v3Manifest = srv.store.getManifest(v3);
  for (const shard of v3Manifest.shards) assert.ok(hashes.has(shard.hash));
  // v1 位置 0 的旧 blob（seed=1）已不在，位置 1（seed=2，被 v3 继承）仍在
  assert.ok(!hashes.has(makeShardHash(1)));
  assert.ok(hashes.has(makeShardHash(2)));
  assert.ok(Array.isArray(rec2.body.deletedBlobs));
});

import { createHash } from 'node:crypto';
function makeShardHash(seed: number): string {
  return createHash('sha256').update(makeShard(seed, SIZE)).digest('hex');
}

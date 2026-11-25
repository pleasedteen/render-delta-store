import { test, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import {
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

const SHARD = 8 * 1024 * 1024; // 8 MiB
const COUNT = 16; // 16 片 => 128 MiB
const LIMIT = 20 * 1024 * 1024; // 峰值增量不超过 20 MiB

/** du -s --block-size=1：取文件系统实际分配块（含临时文件与元数据），单位字节。 */
function allocatedBytes(dir: string): number {
  const out = execFileSync('du', ['-s', '--block-size=1', dir], { encoding: 'utf8' });
  return Number(out.trim().split(/\s+/)[0]);
}

test('128MiB/16×8MiB 差量替换 1 片：创建会话到发布完成，磁盘峰值增量 ≤ 20 MiB', async () => {
  srv = await startTestServer();
  const name = 'asset-128m';

  // ---- 基版：16 片全量发布 ----
  const baseSession = await createFullSession(srv.base, name, SHARD, COUNT);
  for (let p = 0; p < COUNT; p++) {
    await uploadShard(srv.base, baseSession.sid, p, makeShard(p + 1, SHARD));
  }
  const v1 = (await apiJson(srv.base, `/sessions/${baseSession.sid}/publish`, {
    method: 'POST',
  })).body.vid as string;

  const baseline = allocatedBytes(srv.dataDir);

  // ---- 创建差量会话（只替换位置 7）----
  const delta = await apiJson(srv.base, `/objects/${name}/sessions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      mode: 'delta',
      baseVid: v1,
      shardSize: SHARD,
      shardCount: COUNT,
      positions: [7],
    }),
  });
  assert.equal(delta.status, 201);
  const sid = delta.body.sid;

  // ---- 上传新分片的同时高频采样实际磁盘分配 ----
  const newShard = makeShard(4242, SHARD);
  let peak = 0;
  const sampler = setInterval(() => {
    peak = Math.max(peak, allocatedBytes(srv.dataDir) - baseline);
  }, 25);

  await uploadShard(srv.base, sid, 7, newShard);
  // 上传完成后、发布前再采一轮（此时临时文件已 rename 为 blob）
  peak = Math.max(peak, allocatedBytes(srv.dataDir) - baseline);

  const pub = await apiJson(srv.base, `/sessions/${sid}/publish`, { method: 'POST' });
  assert.equal(pub.status, 201);
  clearInterval(sampler);
  peak = Math.max(peak, allocatedBytes(srv.dataDir) - baseline);

  // ---- 断言 ----
  assert.ok(
    peak <= LIMIT,
    `磁盘峰值增量 ${peak} 字节 (${(peak / 1024 / 1024).toFixed(2)} MiB) 超过 20 MiB 限制`,
  );

  // 发布完成后的稳态增量：只有 1 个新 blob（8 MiB）+ 元数据
  const steady = allocatedBytes(srv.dataDir) - baseline;
  assert.ok(
    steady <= SHARD + 1024 * 1024,
    `稳态增量 ${steady} 超出一个分片加 1 MiB 元数据余量`,
  );

  // 正确性：新版本 16 片内容逐字节正确（位置 7 为新片，其余继承基版）
  const res = await fetch(
    `${srv.base}/objects/${name}/versions/${pub.body.vid}/content`,
  );
  assert.equal(res.status, 200);
  const full = Buffer.from(await res.arrayBuffer());
  assert.equal(full.length, SHARD * COUNT);
  for (let p = 0; p < COUNT; p++) {
    const expected = p === 7 ? newShard : makeShard(p + 1, SHARD);
    assert.ok(
      full.subarray(p * SHARD, (p + 1) * SHARD).equals(expected),
      `位置 ${p} 内容不匹配`,
    );
  }

  // 基版仍完整可下载（superseded 但未回收）
  const old = await fetch(`${srv.base}/objects/${name}/versions/${v1}/content`);
  assert.equal(old.status, 200);
  assert.equal((await old.arrayBuffer()).byteLength, SHARD * COUNT);
});

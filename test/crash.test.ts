import { test, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { ChildProcess } from 'node:child_process';
import {
  api,
  apiJson,
  createFullSession,
  hashBuffer,
  makeShard,
  spawnServer,
  uploadShard,
  waitPort,
} from './helpers.js';

const N = 4;
const SIZE = 32 * 1024;

let dataDir: string;
let proc: ChildProcess | null = null;

async function freshDir(): Promise<string> {
  dataDir = await mkdtemp(join(tmpdir(), 'objstore-crash-'));
  return dataDir;
}

async function killHard(p: ChildProcess): Promise<void> {
  p.kill('SIGKILL');
  await new Promise((r) => p.on('exit', r));
}

/**
 * 同步挂上 rejection 处理器，避免进程被 SIGKILL 后 fetch 失败先触发
 * unhandledRejection（node:test 会据此直接判失败），再由测试体断言错误。
 */
function captureFailure(p: Promise<unknown>): Promise<unknown> {
  return p.then(
    () => new Error('预期应当失败但成功了'),
    (e: unknown) => e,
  );
}

afterEach(async () => {
  if (proc && proc.exitCode === null) {
    const p = proc;
    p.kill('SIGKILL');
    await new Promise((r) => p.on('exit', r));
  }
  if (dataDir) await rm(dataDir, { recursive: true, force: true });
});

async function setupBaseVersion(base: string, name: string): Promise<string> {
  const { sid } = await createFullSession(base, name, SIZE, N);
  for (let p = 0; p < N; p++) await uploadShard(base, sid, p, makeShard(p + 1, SIZE));
  return (await apiJson(base, `/sessions/${sid}/publish`, { method: 'POST' })).body.vid;
}

test('上传途中 SIGKILL：已确认分片仍可续传，临时残留被清理，最终发布成功', async () => {
  const dir = await freshDir();
  const spawn1 = spawnServer(dir);
  proc = spawn1.proc;
  const port1 = await spawn1.port;
  const base = `http://127.0.0.1:${port1}`;

  const { sid } = await createFullSession(base, 'crash-1', SIZE, N);
  await uploadShard(base, sid, 0, makeShard(1, SIZE));
  await uploadShard(base, sid, 1, makeShard(2, SIZE));

  // 第三个分片上传过程中直接 SIGKILL（不经过故障注入点，模拟最粗暴的断电）
  const uploadFailure = captureFailure(uploadShard(base, sid, 2, makeShard(3, SIZE)));
  await killHard(proc);
  proc = null;
  const f1 = await uploadFailure;
  assert.ok(f1 instanceof Error, '请求应因进程被杀而失败');

  // 磁盘检查：tmp 下可能有残留，blobs 下只有已确认的两个
  const tmpBefore = await readdir(join(dir, 'tmp'));
  assert.ok(tmpBefore.length <= 1, '崩溃最多留下一个上传临时文件');

  // 重启：自动清理残留并恢复会话
  const spawn2 = spawnServer(dir);
  proc = spawn2.proc;
  const port2 = await spawn2.port;
  await waitPort(port2);
  const base2 = `http://127.0.0.1:${port2}`;

  const tmpAfter = await readdir(join(dir, 'tmp'));
  assert.deepEqual(tmpAfter, [], '启动恢复清理临时残留');

  const view = (await apiJson(base2, `/objects/crash-1/sessions/${sid}`)).body;
  assert.deepEqual(view.confirmed, [0, 1]);
  assert.deepEqual(view.pending, [2, 3]);

  // 续传（含位置 2 的原样重传）并发布
  await uploadShard(base2, sid, 2, makeShard(3, SIZE));
  await uploadShard(base2, sid, 3, makeShard(4, SIZE));
  const pub = await apiJson(base2, `/sessions/${sid}/publish`, { method: 'POST' });
  assert.equal(pub.status, 201);

  const res = await api(base2, `/objects/crash-1/versions/${pub.body.vid}/content`);
  const got = Buffer.from(await res.arrayBuffer());
  assert.ok(
    got.equals(Buffer.concat([0, 1, 2, 3].map((p) => makeShard(p + 1, SIZE)))),
  );
});

test('blob 已写、会话元数据更新前崩溃：重启后该位置未确认，重试成功且不产生重复 blob', async () => {
  const dir = await freshDir();
  const spawn1 = spawnServer(dir, { FAULT_CRASH: 'after_blob_write' });
  proc = spawn1.proc;
  const port1 = await spawn1.port;
  const base = `http://127.0.0.1:${port1}`;

  const { sid } = await createFullSession(base, 'crash-2', SIZE, N);
  const data = makeShard(1, SIZE);
  const uploadFailure = captureFailure(uploadShard(base, sid, 0, data));
  await new Promise((r) => proc!.on('exit', r));
  proc = null;
  const f2 = await uploadFailure;
  assert.ok(f2 instanceof Error);

  // blob 已落盘但会话未确认 -> 孤儿 blob
  const blobDir = join(dir, 'blobs', hashBuffer(data).slice(0, 2));
  const files = await readdir(blobDir);
  assert.ok(files.includes(hashBuffer(data)));

  const spawn2 = spawnServer(dir);
  proc = spawn2.proc;
  const port2 = await spawn2.port;
  await waitPort(port2);
  const base2 = `http://127.0.0.1:${port2}`;

  const view = (await apiJson(base2, `/objects/crash-2/sessions/${sid}`)).body;
  assert.deepEqual(view.confirmed, []);

  // 重传：blob 去重命中，不重复占盘，确认成功
  const retry = await uploadShard(base2, sid, 0, data);
  assert.equal(retry.status, 201);
  const after = await readdir(blobDir);
  assert.deepEqual(after, [hashBuffer(data)]);
});

test('发布途中崩溃（索引切换前）：当前版本保持完整旧版；重启后可重新发布得到同一 vid', async () => {
  const dir = await freshDir();
  const spawn1 = spawnServer(dir);
  proc = spawn1.proc;
  const port1 = await spawn1.port;
  const base = `http://127.0.0.1:${port1}`;
  const v1 = await setupBaseVersion(base, 'crash-3');

  // 差量会话替换位置 1
  const create = await apiJson(base, '/objects/crash-3/sessions', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ mode: 'delta', baseVid: v1, shardSize: SIZE, shardCount: N, positions: [1] }),
  });
  const sid = create.body.sid;
  await uploadShard(base, sid, 1, makeShard(77, SIZE));

  // 在“索引原子切换前”崩溃：清单可能已写，但 currentVid 必须仍是 v1
  await killHard(proc);
  proc = null;

  const spawn2 = spawnServer(dir, { FAULT_CRASH: 'before_index_swap' });
  proc = spawn2.proc;
  const port2 = await spawn2.port;
  await waitPort(port2);
  const base2 = `http://127.0.0.1:${port2}`;

  const objBefore = (await apiJson(base2, '/objects/crash-3')).body;
  assert.equal(objBefore.currentVid, v1, '崩溃后当前版本只能是完整旧版');
  assert.equal(objBefore.versions.length, 1);

  // 重新发布（故障注入仍在：会在索引切换前再次崩溃）
  const pubFailure = captureFailure(
    apiJson(base2, `/sessions/${sid}/publish`, { method: 'POST' }),
  );
  await new Promise((r) => proc!.on('exit', r));
  proc = null;
  const f3 = await pubFailure;
  assert.ok(f3 instanceof Error);

  // 再次重启（不带故障注入），恢复流程补齐索引
  const spawn3 = spawnServer(dir);
  proc = spawn3.proc;
  const port3 = await spawn3.port;
  await waitPort(port3);
  const base3 = `http://127.0.0.1:${port3}`;

  // 会话仍处于 open（发布最后一步未完成），再次发布成功
  const pub = await apiJson(base3, `/sessions/${sid}/publish`, { method: 'POST' });
  assert.equal(pub.status, 201);
  const v2 = pub.body.vid;

  const objAfter = (await apiJson(base3, '/objects/crash-3')).body;
  assert.equal(objAfter.currentVid, v2);
  assert.equal(objAfter.versions.length, 2);

  // 确定性 vid：与恢复期间生成的清单文件名一致
  const manifestFiles = await readdir(join(dir, 'versions'));
  assert.ok(manifestFiles.includes(`${v2}.json`));

  // 内容正确
  const got = Buffer.from(
    await (await api(base3, `/objects/crash-3/versions/${v2}/content`)).arrayBuffer(),
  );
  const expected = Buffer.concat(
    [0, 1, 2, 3].map((p) => (p === 1 ? makeShard(77, SIZE) : makeShard(p + 1, SIZE))),
  );
  assert.ok(got.equals(expected));

  // 旧版完整可下载
  const old = Buffer.from(
    await (await api(base3, `/objects/crash-3/versions/${v1}/content`)).arrayBuffer(),
  );
  assert.ok(old.equals(Buffer.concat([0, 1, 2, 3].map((p) => makeShard(p + 1, SIZE)))));
});

test('全部落盘完成、HTTP 响应返回前崩溃：启动恢复自动补齐，重复发布返回同一 vid', async () => {
  const dir = await freshDir();
  // 基版在无故障注入的进程中发布
  const spawn0 = spawnServer(dir);
  proc = spawn0.proc;
  const port0 = await spawn0.port;
  const base0 = `http://127.0.0.1:${port0}`;
  const v1 = await setupBaseVersion(base0, 'crash-4');
  const create = await apiJson(base0, '/objects/crash-4/sessions', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ mode: 'delta', baseVid: v1, shardSize: SIZE, shardCount: N, positions: [2] }),
  });
  const sid = create.body.sid;
  await uploadShard(base0, sid, 2, makeShard(88, SIZE));
  await killHard(proc);
  proc = null;

  // 带故障注入重启：发布在会话元数据落盘之后、响应返回前被 SIGKILL
  const spawn1 = spawnServer(dir, { FAULT_CRASH: 'after_session_publish' });
  proc = spawn1.proc;
  const port1 = await spawn1.port;
  const base = `http://127.0.0.1:${port1}`;
  const pubFailure = captureFailure(
    apiJson(base, `/sessions/${sid}/publish`, { method: 'POST' }),
  );
  await new Promise((r) => proc!.on('exit', r));
  proc = null;
  const f4 = await pubFailure;
  assert.ok(f4 instanceof Error, '响应因进程被杀而丢失');

  // 不带注入重启：恢复路径扫描已发布会话并补齐索引
  const spawn2 = spawnServer(dir);
  proc = spawn2.proc;
  const port2 = await spawn2.port;
  await waitPort(port2);
  const base2 = `http://127.0.0.1:${port2}`;

  const view = (await apiJson(base2, `/objects/crash-4/sessions/${sid}`)).body;
  assert.equal(view.status, 'published');
  assert.ok(view.publishedVid);

  // 重复发布必须返回第一次的版本标识
  const again = await apiJson(base2, `/sessions/${sid}/publish`, { method: 'POST' });
  assert.equal(again.status, 200);
  assert.equal(again.body.idempotent, true);
  assert.equal(again.body.vid, view.publishedVid);

  const obj = (await apiJson(base2, '/objects/crash-4')).body;
  assert.equal(obj.currentVid, view.publishedVid);
  assert.equal(obj.versions.length, 2);
  const cur = Buffer.from(
    await (await api(base2, `/objects/crash-4/versions/${obj.currentVid}/content`)).arrayBuffer(),
  );
  const expected = Buffer.concat(
    [0, 1, 2, 3].map((p) => (p === 2 ? makeShard(88, SIZE) : makeShard(p + 1, SIZE))),
  );
  assert.ok(cur.equals(expected));
});

test('元数据文件本身保持合法 JSON（原子写不留半成品）', async () => {
  const dir = await freshDir();
  const spawn1 = spawnServer(dir, { FAULT_CRASH: 'before_session_update' });
  proc = spawn1.proc;
  const port1 = await spawn1.port;
  const base = `http://127.0.0.1:${port1}`;
  const { sid } = await createFullSession(base, 'crash-5', SIZE, N);
  const uploadFailure = captureFailure(uploadShard(base, sid, 0, makeShard(1, SIZE)));
  await new Promise((r) => proc!.on('exit', r));
  proc = null;
  const f5 = await uploadFailure;
  assert.ok(f5 instanceof Error);

  // 所有会话/对象 JSON 均可解析
  const sessionRaw = await readFile(join(dir, 'sessions', `${sid}.json`), 'utf8');
  const parsed = JSON.parse(sessionRaw);
  assert.equal(parsed.sid, sid);
  assert.deepEqual(parsed.uploaded, {}, '确认未发生，元数据停留在上一个一致状态');
});

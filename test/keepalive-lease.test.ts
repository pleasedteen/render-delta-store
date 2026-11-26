import { test, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { Agent, request, type IncomingMessage } from 'node:http';
import {
  api,
  apiJson,
  createFullSession,
  hashBuffer,
  makeShard,
  startTestServer,
  uploadShard,
  type TestServer,
} from './helpers.js';

let srv: TestServer;
const agents: Agent[] = [];
afterEach(async () => {
  for (const a of agents) a.destroy();
  agents.length = 0;
  if (srv) await srv.close();
});

const N = 4;
// 5MiB 分片：整包响应 20MiB、跨片 Range 15MiB，远超本机内核 socket 缓冲上限
// （客户端 rcvbuf ≤ 6MiB + 服务端 sndbuf ≤ 4MiB，见 tcp_rmem/tcp_wmem），
// 保证客户端暂停接收时服务端写不完、响应确实仍在途中（占用保持有效）。
const SIZE = 5 * 1024 * 1024;

/** 单条 keep-alive 连接（maxSockets=1 保证顺序请求复用同一 socket）。 */
function keepAliveAgent(): Agent {
  const agent = new Agent({ keepAlive: true, maxSockets: 1 });
  agents.push(agent);
  return agent;
}

async function publish(name: string, offset: number): Promise<string> {
  const { sid } = await createFullSession(srv.base, name, SIZE, N);
  for (let p = 0; p < N; p++) await uploadShard(srv.base, sid, p, makeShard(p + offset, SIZE));
  return (await apiJson(srv.base, `/sessions/${sid}/publish`, { method: 'POST' })).body.vid;
}

/** 位置 p（offset=1 发布的 v1）对应的 blob hash。 */
const v1Hash = (p: number) => hashBuffer(makeShard(p + 1, SIZE));
const expectedV1 = () => Buffer.concat([...Array(N).keys()].map((p) => makeShard(p + 1, SIZE)));
const expectedV2 = () =>
  Buffer.concat([...Array(N).keys()].map((p) => makeShard(p + 100, SIZE)));

function rawGet(
  agent: Agent,
  path: string,
  headers: Record<string, string> = {},
  opts: { pause?: boolean } = {},
): Promise<IncomingMessage> {
  const url = new URL(srv.base + path);
  return new Promise((resolve, reject) => {
    const req = request(
      {
        agent,
        hostname: url.hostname,
        port: url.port,
        path: url.pathname + url.search,
        method: 'GET',
        headers,
      },
      (res) => {
        // 在 response 回调里同步暂停 socket：TCP 窗口随即关闭，
        // 服务端写不完响应，保证“暂停接收”的语义真实成立。
        if (opts.pause) res.pause();
        resolve(res);
      },
    );
    req.on('error', reject);
    req.end();
  });
}

/** 只读第一块就停住（paused 模式不再 read），模拟渲染进程暂停接收。 */
function readFirstChunk(res: IncomingMessage): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    res.once('error', reject);
    res.once('readable', () => {
      const chunk = res.read() as Buffer | null;
      if (chunk) resolve(chunk);
    });
  });
}

/** 恢复接收并读完剩余部分，返回含 first 的完整字节。 */
function drainRest(res: IncomingMessage, first: Buffer): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [first];
    res.on('data', (c: Buffer) => chunks.push(c));
    res.on('end', () => resolve(Buffer.concat(chunks)));
    res.on('error', reject);
  });
}

/** 慢读：逐块消费且每块之间让出事件循环。 */
async function slowReadAll(res: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of res) {
    chunks.push(chunk as Buffer);
    await new Promise((r) => setTimeout(r, 1));
  }
  return Buffer.concat(chunks);
}

/** 丢弃（错误）响应体，让 keep-alive 连接可复用。 */
async function discard(res: IncomingMessage): Promise<void> {
  res.resume();
  await new Promise((resolve, reject) => {
    res.on('end', resolve);
    res.on('error', reject);
  });
}

async function waitFor(cond: () => boolean, what: string, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!cond()) {
    if (Date.now() > deadline) throw new Error(`等待超时: ${what}`);
    await new Promise((r) => setTimeout(r, 20));
  }
}

function reclaim(name: string) {
  return apiJson(srv.base, '/reclaim', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ object: name }),
  });
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

test('同一长连接连续下载不同版本：暂停的整包响应在换版+回收后读完，后续请求复用同一连接', async () => {
  srv = await startTestServer();
  const agent = keepAliveAgent();
  const v1 = await publish('ka-a', 1);

  // 响应 1：整包下载 v1，读第一块后暂停接收（连接保持）
  const res1 = await rawGet(agent, `/objects/ka-a/versions/${v1}/content`, {}, { pause: true });
  assert.equal(res1.statusCode, 200);
  const port1 = res1.socket.localPort;
  const first = await readFirstChunk(res1);
  assert.ok(first.length > 0);
  assert.ok(first.length < SIZE * N, '响应应仍在途中（未整体到达）');

  // 暂停期间换版 + 回收：回收顺利完成，但 v1 的分片被在途响应钉住
  const v2 = await publish('ka-a', 100);
  const rec = await reclaim('ka-a');
  assert.ok(rec.body.reclaimedVids.includes(v1));
  assert.deepEqual(
    [...srv.store.heldBlobHashes()].sort(),
    [0, 1, 2, 3].map(v1Hash).sort(),
    '整包响应应钉住 v1 全部分片',
  );
  assert.ok(!rec.body.deletedBlobs.some((h: string) => [0, 1, 2, 3].map(v1Hash).includes(h)));

  // 换版与回收完成后恢复接收：已开始的响应必须完整读完、字节精确
  const all = await drainRest(res1, first);
  assert.ok(all.equals(expectedV1()), '暂停的响应在换版+回收后读完整个原版本');
  await waitFor(() => srv.store.heldBlobHashes().size === 0, '响应结束后占用释放');

  // 同一连接上：旧版已失效，新请求 -> 410
  const gone = await rawGet(agent, `/objects/ka-a/versions/${v1}/content`);
  assert.equal(gone.statusCode, 410);
  assert.equal(gone.socket.localPort, port1, '后续请求复用同一连接');
  await discard(gone);

  // 同一连接上：下载当前版本 v2，状态与字节精确
  const res2 = await rawGet(agent, `/objects/ka-a/versions/${v2}/content`);
  assert.equal(res2.statusCode, 200);
  assert.equal(res2.socket.localPort, port1, '连续下载不同版本复用同一连接');
  const body2 = await slowReadAll(res2);
  assert.ok(body2.equals(expectedV2()));

  // 响应 1 结束后其占用的分片在下一轮即可回收
  const rec2 = await reclaim('ka-a');
  const deleted2 = new Set(rec2.body.deletedBlobs);
  for (const p of [0, 1, 2, 3]) assert.ok(deleted2.has(v1Hash(p)), `v1 位置 ${p} 应被回收`);
  const stats = await srv.store.blobStats();
  assert.equal(stats.count, N, '磁盘上只剩 v2 的分片');
});

test('显式租约中途释放：已开始的暂停响应仍读完；旧版停发新租约、失效后不可再请求', async () => {
  srv = await startTestServer();
  const agent = keepAliveAgent();
  const v1 = await publish('ka-b', 1);
  const lease = (
    await apiJson(srv.base, `/objects/ka-b/versions/${v1}/leases`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ ttlSeconds: 600 }),
    })
  ).body;

  // 响应 1：持租约整包下载 v1，读第一块后暂停
  const res1 = await rawGet(
    agent,
    `/objects/ka-b/versions/${v1}/content?lease=${lease.lid}`,
    {},
    { pause: true },
  );
  assert.equal(res1.statusCode, 200);
  const port1 = res1.socket.localPort;
  const first = await readFirstChunk(res1);

  // 换版 + 回收，旧版撤下
  const v2 = await publish('ka-b', 100);
  await reclaim('ka-b');

  // 原有租约仍有效：可用 Range 续传旧版（另一条连接）
  const resume = await fetch(`${srv.base}/objects/ka-b/versions/${v1}/content?lease=${lease.lid}`, {
    headers: { range: `bytes=${SIZE}-${SIZE + 99}` },
  });
  assert.equal(resume.status, 206);
  assert.equal(resume.headers.get('content-range'), `bytes ${SIZE}-${SIZE + 99}/${SIZE * N}`);
  assert.ok(Buffer.from(await resume.arrayBuffer()).equals(expectedV1().subarray(SIZE, SIZE + 100)));

  // 租约中途释放 + 再次回收：在途响应的分片仍受响应级占用保护
  assert.equal((await api(srv.base, `/leases/${lease.lid}`, { method: 'DELETE' })).status, 200);
  await reclaim('ka-b');
  assert.ok(srv.store.heldBlobHashes().size > 0, '租约释放后在途响应仍持有分片');

  // 租约已释放，已开始的响应依然完整读完
  const all = await drainRest(res1, first);
  assert.ok(all.equals(expectedV1()), '显式租约中途释放，已开始的响应仍读完');

  // 旧版撤下后停止签发新租约
  const noLease = await apiJson(srv.base, `/objects/ka-b/versions/${v1}/leases`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ ttlSeconds: 60 }),
  });
  assert.equal(noLease.status, 410);

  // 租约失效后不能再对旧版发起请求（同一连接）：带旧租约 / 不带租约都 410
  const stale = await rawGet(agent, `/objects/ka-b/versions/${v1}/content?lease=${lease.lid}`);
  assert.equal(stale.statusCode, 410);
  assert.equal(stale.socket.localPort, port1);
  await discard(stale);
  const bare = await rawGet(agent, `/objects/ka-b/versions/${v1}/content`);
  assert.equal(bare.statusCode, 410);
  await discard(bare);

  // 同一连接下载当前版本不受影响
  const res2 = await rawGet(agent, `/objects/ka-b/versions/${v2}/content`);
  assert.equal(res2.statusCode, 200);
  assert.equal(res2.socket.localPort, port1);
  assert.ok((await slowReadAll(res2)).equals(expectedV2()));

  // 响应结束 + 租约释放后，下一轮回收清掉 v1 全部分片
  await waitFor(() => srv.store.heldBlobHashes().size === 0, '响应结束后占用释放');
  await reclaim('ka-b');
  const stats = await srv.store.blobStats();
  for (const p of [0, 1, 2, 3]) {
    assert.ok(!stats.blobs.some((b) => b.hash === v1Hash(p)), `v1 位置 ${p} 应被回收`);
  }
});

test('显式租约中途过期：已开始的暂停响应仍读完，过期后旧版拒绝新请求', async () => {
  srv = await startTestServer();
  const agent = keepAliveAgent();
  const v1 = await publish('ka-c', 1);
  // 0.2s 后过期的租约
  const lease = (
    await apiJson(srv.base, `/objects/ka-c/versions/${v1}/leases`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ ttlSeconds: 0.2 }),
    })
  ).body;

  // 租约有效期内开始下载，读第一块后暂停
  const res1 = await rawGet(
    agent,
    `/objects/ka-c/versions/${v1}/content?lease=${lease.lid}`,
    {},
    { pause: true },
  );
  assert.equal(res1.statusCode, 200);
  const first = await readFirstChunk(res1);

  // 等租约过期，然后换版 + 回收
  await sleep(400);
  await publish('ka-c', 100);
  await reclaim('ka-c');

  // 租约已过期，已开始的响应依然完整读完
  const all = await drainRest(res1, first);
  assert.ok(all.equals(expectedV1()), '显式租约中途过期，已开始的响应仍读完');

  // 过期租约不能再对旧版发起请求
  const stale = await rawGet(agent, `/objects/ka-c/versions/${v1}/content?lease=${lease.lid}`);
  assert.equal(stale.statusCode, 410);
  await discard(stale);

  // 过期租约不再钉住旧版：响应结束后下一轮回收清掉 v1
  await waitFor(() => srv.store.heldBlobHashes().size === 0, '响应结束后占用释放');
  await reclaim('ka-c');
  const stats = await srv.store.blobStats();
  for (const p of [0, 1, 2, 3]) {
    assert.ok(!stats.blobs.some((b) => b.hash === v1Hash(p)), `v1 位置 ${p} 应被回收`);
  }
});

test('暂停的 Range 只占用本次响应所需分片：其余当轮回收，中断后下一轮回收占用片', async () => {
  srv = await startTestServer();
  const agent = keepAliveAgent();
  const v1 = await publish('ka-d', 1);

  // Range 精确覆盖第 1..3 片 [SIZE, 4*SIZE-1]（15MiB，暂停时服务端写不完），
  // 第 0 片不在本次响应的字节区间内。
  const res1 = await rawGet(
    agent,
    `/objects/ka-d/versions/${v1}/content`,
    { range: `bytes=${SIZE}-${N * SIZE - 1}` },
    { pause: true },
  );
  assert.equal(res1.statusCode, 206);
  assert.equal(res1.headers['content-range'], `bytes ${SIZE}-${N * SIZE - 1}/${SIZE * N}`);
  const first = await readFirstChunk(res1);
  assert.ok(first.length > 0);

  // 响应只钉住区间内的第 1/2/3 片，而不是整个版本
  assert.deepEqual(
    [...srv.store.heldBlobHashes()].sort(),
    [1, 2, 3].map(v1Hash).sort(),
  );

  // 换版 + 回收：无引用的第 0 片当轮即被回收，被占用的 1/2/3 片保留
  await publish('ka-d', 100);
  const rec = await reclaim('ka-d');
  const deleted = new Set(rec.body.deletedBlobs);
  assert.ok(deleted.has(v1Hash(0)), '无引用的位置 0 当轮回收');
  for (const p of [1, 2, 3]) assert.ok(!deleted.has(v1Hash(p)), `被占用位置 ${p} 当轮保留`);
  let stats = await srv.store.blobStats();
  for (const p of [1, 2, 3]) {
    assert.ok(stats.blobs.some((b) => b.hash === v1Hash(p)), `位置 ${p} 的 blob 仍在磁盘上`);
  }

  // 响应中断后占用释放，下一轮即可回收它占用的分片
  res1.destroy();
  await waitFor(() => srv.store.heldBlobHashes().size === 0, '响应中断后占用释放');
  const rec2 = await reclaim('ka-d');
  for (const p of [1, 2, 3]) assert.ok(rec2.body.deletedBlobs.includes(v1Hash(p)));
  stats = await srv.store.blobStats();
  assert.equal(stats.count, N, '磁盘上只剩 v2 的分片');
});

test('复用连接上的慢读：整包/单片/跨片 Range 的状态码与字节区间精确', async () => {
  srv = await startTestServer();
  const agent = keepAliveAgent();
  const v1 = await publish('ka-e', 1);
  const total = SIZE * N;
  const full = expectedV1();

  // 整包：200 + 完整字节（慢读）
  const r1 = await rawGet(agent, `/objects/ka-e/versions/${v1}/content`);
  assert.equal(r1.statusCode, 200);
  assert.equal(Number(r1.headers['content-length']), total);
  assert.equal(r1.headers['accept-ranges'], 'bytes');
  const port1 = r1.socket.localPort;
  assert.ok((await slowReadAll(r1)).equals(full));

  // 单片：200 + 单片字节
  const r2 = await rawGet(agent, `/objects/ka-e/versions/${v1}/shards/2`);
  assert.equal(r2.statusCode, 200);
  assert.equal(r2.socket.localPort, port1, '请求复用同一连接');
  assert.equal(Number(r2.headers['content-length']), SIZE);
  assert.ok((await slowReadAll(r2)).equals(makeShard(3, SIZE)));

  // 跨片 Range：206 + 精确 Content-Range 与字节
  const start = SIZE - 10;
  const end = SIZE + 20;
  const r3 = await rawGet(agent, `/objects/ka-e/versions/${v1}/content`, {
    range: `bytes=${start}-${end}`,
  });
  assert.equal(r3.statusCode, 206);
  assert.equal(r3.socket.localPort, port1);
  assert.equal(r3.headers['content-range'], `bytes ${start}-${end}/${total}`);
  assert.ok((await slowReadAll(r3)).equals(full.subarray(start, end + 1)));

  // 单片上的 Range：206 + 单片内区间
  const r4 = await rawGet(agent, `/objects/ka-e/versions/${v1}/shards/3`, {
    range: 'bytes=5-14',
  });
  assert.equal(r4.statusCode, 206);
  assert.equal(r4.headers['content-range'], `bytes 5-14/${SIZE}`);
  assert.ok((await slowReadAll(r4)).equals(makeShard(4, SIZE).subarray(5, 15)));

  // 越界 Range：416
  const r5 = await rawGet(agent, `/objects/ka-e/versions/${v1}/content`, {
    range: `bytes=${total}-${total + 9}`,
  });
  assert.equal(r5.statusCode, 416);
  await discard(r5);
});

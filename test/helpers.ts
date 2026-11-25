import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import type { AddressInfo } from 'node:net';
import { spawn, type ChildProcess } from 'node:child_process';
import { createApp } from '../src/server.js';
import { ObjectStore } from '../src/store.js';

export interface TestServer {
  base: string;
  dataDir: string;
  store: ObjectStore;
  close: () => Promise<void>;
}

/** 启动一个进程内、临时数据目录、随机端口的测试服务。 */
export async function startTestServer(): Promise<TestServer> {
  const dataDir = await mkdtemp(join(tmpdir(), 'objstore-'));
  const store = new ObjectStore(dataDir);
  await store.init();
  const app = createApp(store);
  await new Promise<void>((resolve) => app.listen(0, '127.0.0.1', () => resolve()));
  const port = (app.address() as AddressInfo).port;
  return {
    base: `http://127.0.0.1:${port}`,
    dataDir,
    store,
    close: async () => {
      await new Promise<void>((resolve) => app.close(() => resolve()));
      await rm(dataDir, { recursive: true, force: true });
    },
  };
}

/** 子进程方式启动服务（故障注入/强制终止测试用）。 */
export function spawnServer(
  dataDir: string,
  env: NodeJS.ProcessEnv = {},
): { proc: ChildProcess; port: Promise<number> } {
  const proc = spawn(
    process.execPath,
    [
      '--import',
      'tsx',
      join(import.meta.dirname, '..', 'src', 'main.ts'),
      '--port',
      '0',
      '--data',
      dataDir,
    ],
    {
      env: { ...process.env, ...env },
      stdio: ['ignore', 'pipe', 'inherit'],
    },
  );
  const port = new Promise<number>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('子进程启动超时')), 15000);
    proc.stdout!.on('data', (chunk: Buffer) => {
      const m = /"url":\s*"http:\/\/[^:]+:(\d+)"/.exec(chunk.toString());
      if (m) {
        clearTimeout(timer);
        resolve(Number(m[1]));
      }
    });
    proc.on('exit', (code) => {
      clearTimeout(timer);
      reject(new Error(`子进程意外退出 code=${code}`));
    });
  });
  return { proc, port };
}

export async function waitPort(port: number, timeoutMs = 10000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`http://127.0.0.1:${port}/health`);
      if (res.ok) return;
    } catch {
      await new Promise((r) => setTimeout(r, 100));
    }
  }
  throw new Error(`端口 ${port} 健康检查超时`);
}

/**
 * 确定性伪随机数据：相同 seed 生成相同字节，便于校验重建后内容逐字节一致。
 * seed 通过初始状态与每轮混合进入输出，不同 seed 不会碰撞（旧实现曾出现
 * makeShard(88) 与 makeShard(3) 同内容的情况）。
 */
export function makeShard(seed: number, size: number): Buffer {
  const buf = Buffer.alloc(size);
  let state = (seed ^ 0x9e3779b9) >>> 0;
  for (let i = 0; i < size; i += 4) {
    // xorshift32，并把位置与 seed 混入，保证不同 seed 的整片字节不同
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    state = (state + Math.imul(seed + 1, i + 1)) >>> 0;
    buf.writeUInt32LE(state >>> 0, i);
  }
  return buf;
}

export function hashBuffer(buf: Buffer): string {
  return createHash('sha256').update(buf).digest('hex');
}

export async function api(
  base: string,
  path: string,
  init: RequestInit = {},
): Promise<Response> {
  const res = await fetch(`${base}${path}`, init);
  return res;
}

export async function apiJson<T = any>(
  base: string,
  path: string,
  init: RequestInit = {},
): Promise<{ status: number; body: T }> {
  const res = await api(base, path, init);
  const body = (await res.json()) as T;
  return { status: res.status, body };
}

export async function createFullSession(
  base: string,
  name: string,
  shardSize: number,
  shardCount: number,
): Promise<{ sid: string }> {
  const { body } = await apiJson(base, `/objects/${name}/sessions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ mode: 'full', shardSize, shardCount }),
  });
  return body as { sid: string };
}

export async function uploadShard(
  base: string,
  sid: string,
  position: number,
  data: Buffer,
): Promise<Response> {
  return api(base, `/sessions/${sid}/shards/${position}`, {
    method: 'PUT',
    headers: {
      'content-type': 'application/octet-stream',
      'x-shard-hash': hashBuffer(data),
    },
    body: new Uint8Array(data),
  });
}

export async function downloadAll(base: string, name: string, vid: string, lease?: string): Promise<Buffer> {
  const suffix = lease ? `?lease=${lease}` : '';
  const res = await api(base, `/objects/${name}/versions/${vid}/content${suffix}`);
  if (!res.ok) throw new Error(`下载失败 ${res.status}: ${await res.text()}`);
  return Buffer.from(await res.arrayBuffer());
}

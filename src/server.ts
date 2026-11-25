import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { HttpError } from './util/errors.js';
import { readJsonBody, sendError, sendJson } from './http-util.js';
import type { ObjectStore } from './store.js';
import type { VersionManifest } from './types.js';

type Action =
  | 'health'
  | 'list_objects'
  | 'create_session'
  | 'get_session'
  | 'put_shard'
  | 'publish'
  | 'get_object'
  | 'create_lease'
  | 'release_lease'
  | 'get_content'
  | 'get_shard'
  | 'reclaim';

interface RouteDef {
  method: string;
  pattern: string;
  action: Action;
}

const ROUTES: RouteDef[] = [
  { method: 'GET', pattern: '/health', action: 'health' },
  { method: 'GET', pattern: '/objects', action: 'list_objects' },
  { method: 'POST', pattern: '/objects/:name/sessions', action: 'create_session' },
  { method: 'GET', pattern: '/objects/:name/sessions/:sid', action: 'get_session' },
  { method: 'PUT', pattern: '/sessions/:sid/shards/:position', action: 'put_shard' },
  { method: 'POST', pattern: '/sessions/:sid/publish', action: 'publish' },
  { method: 'GET', pattern: '/objects/:name', action: 'get_object' },
  { method: 'POST', pattern: '/objects/:name/versions/:vid/leases', action: 'create_lease' },
  { method: 'DELETE', pattern: '/leases/:lid', action: 'release_lease' },
  { method: 'GET', pattern: '/objects/:name/versions/:vid/content', action: 'get_content' },
  { method: 'GET', pattern: '/objects/:name/versions/:vid/shards/:position', action: 'get_shard' },
  { method: 'POST', pattern: '/reclaim', action: 'reclaim' },
];

interface Match {
  action: Action;
  params: Record<string, string>;
}

function matchRoute(method: string, pathname: string): Match | null {
  for (const r of ROUTES) {
    if (r.method !== method) continue;
    const keys: string[] = [];
    const regex = new RegExp(
      `^${r.pattern.replace(/:([A-Za-z]+)/g, (_m, k: string) => {
        keys.push(k);
        return '([^/]+)';
      })}$`,
    );
    const m = regex.exec(pathname);
    if (!m) continue;
    const params: Record<string, string> = {};
    keys.forEach((k, i) => {
      params[k] = decodeURIComponent(m[i + 1]!);
    });
    return { action: r.action, params };
  }
  return null;
}

export function createApp(store: ObjectStore) {
  return createServer(async (req: IncomingMessage, res: ServerResponse) => {
    try {
      const url = new URL(req.url ?? '/', 'http://localhost');
      const match = matchRoute(req.method ?? '', url.pathname);
      if (!match) {
        sendError(res, 404, 'not_found', `无此路由: ${req.method} ${url.pathname}`);
        return;
      }
      const { action, params } = match;
      const query = url.searchParams;

      switch (action) {
        case 'health':
          sendJson(res, 200, { ok: true, root: store.root });
          return;

        case 'list_objects':
          sendJson(res, 200, { objects: store.listObjects() });
          return;

        case 'create_session': {
          const body = await readJsonBody(req);
          const view = await store.createSession({
            object: params.name!,
            mode: body.mode === 'delta' ? 'delta' : 'full',
            shardSize: Number(body.shardSize),
            shardCount: Number(body.shardCount),
            totalSize: body.totalSize === undefined ? undefined : Number(body.totalSize),
            positions: body.positions,
            baseVid: body.baseVid,
          });
          sendJson(res, 201, view);
          return;
        }

        case 'get_session':
          sendJson(res, 200, store.getSessionView(params.sid!));
          return;

        case 'put_shard': {
          const position = Number(params.position);
          const declaredHash = req.headers['x-shard-hash'] as string | undefined;
          const contentLength =
            req.headers['content-length'] === undefined
              ? undefined
              : Number(req.headers['content-length']);
          const result = await store.putShard(params.sid!, position, req, {
            hash: declaredHash,
            contentLength,
          });
          sendJson(res, result.idempotent ? 200 : 201, {
            sid: params.sid,
            position,
            shard: result.shard,
            confirmed: true,
            idempotent: result.idempotent,
          });
          return;
        }

        case 'publish': {
          const result = await store.publish(params.sid!);
          sendJson(res, result.idempotent ? 200 : 201, result);
          return;
        }

        case 'get_object':
          sendJson(res, 200, store.getObjectView(params.name!));
          return;

        case 'create_lease': {
          const body = await readJsonBody(req).catch(() => ({}));
          const ttl =
            body.ttlSeconds === undefined || body.ttlSeconds === null
              ? null
              : Number(body.ttlSeconds);
          const lease = store.createLease(params.name!, params.vid!, ttl);
          sendJson(res, 201, lease);
          return;
        }

        case 'release_lease':
          store.releaseLease(params.lid!);
          sendJson(res, 200, { released: params.lid });
          return;

        case 'reclaim': {
          const body = await readJsonBody(req).catch(() => ({}));
          const result = await store.reclaim(body.object ?? null);
          sendJson(res, 200, result);
          return;
        }

        case 'get_content':
          await streamContent(store, req, res, params.name!, params.vid!, query, false);
          return;

        case 'get_shard':
          await streamContent(
            store,
            req,
            res,
            params.name!,
            params.vid!,
            query,
            Number(params.position),
          );
          return;
      }
    } catch (err) {
      handleError(err, res);
    }
  });
}

function handleError(err: unknown, res: ServerResponse): void {
  if (res.headersSent) {
    res.destroy();
    return;
  }
  if (err instanceof HttpError) {
    sendError(res, err.status, err.code, err.message);
    return;
  }
  const anyErr = err as { status?: number; message?: string };
  if (anyErr?.status === 413) {
    sendError(res, 413, 'payload_too_large', '请求体过大');
    return;
  }
  if (anyErr?.status === 400) {
    sendError(res, 400, 'bad_request', '请求体不是合法 JSON');
    return;
  }
  // eslint-disable-next-line no-console
  console.error('[unhandled]', err);
  sendError(res, 500, 'internal_error', anyErr?.message ?? '内部错误');
}

interface RangeSpec {
  start: number;
  end: number; // 含端点
}

function parseRange(header: string | undefined, total: number): RangeSpec | null {
  if (!header) return null;
  const m = /^bytes=(\d*)-(\d*)$/.exec(header.trim());
  if (!m) return null;
  const startStr = m[1];
  const endStr = m[2];
  let start: number;
  let end: number;
  if (startStr === '') {
    // suffix range: bytes=-N
    const suffix = Number(endStr);
    if (!Number.isFinite(suffix) || suffix <= 0) return null;
    start = Math.max(0, total - suffix);
    end = total - 1;
  } else {
    start = Number(startStr);
    end = endStr === '' ? total - 1 : Number(endStr);
  }
  if (!Number.isFinite(start) || !Number.isFinite(end) || start > end || start >= total) {
    return null;
  }
  return { start, end: Math.min(end, total - 1) };
}

async function streamContent(
  store: ObjectStore,
  req: IncomingMessage,
  res: ServerResponse,
  name: string,
  vid: string,
  query: URLSearchParams,
  onlyPosition: number | false,
): Promise<void> {
  const leaseId = query.get('lease');
  const manifest: VersionManifest = store.requireDownloadable(vid, leaseId);

  // 连接级隐式租约：暂停接收（不关闭连接）期间，发布与回收都不会删掉这些 blob。
  if (!leaseId) store.attachConnectionLease(req.socket, name, vid);

  let total: number;
  let ordered: Array<{ hash: string; size: number; position: number }>;
  if (onlyPosition === false) {
    total = manifest.totalSize;
    ordered = manifest.shards.map((s) => ({
      hash: s.hash,
      size: s.size,
      position: s.position,
    }));
  } else {
    const shard = manifest.shards.find((s) => s.position === onlyPosition);
    if (!shard) throw new HttpError(404, 'not_found', `分片位置不存在: ${onlyPosition}`);
    total = shard.size;
    ordered = [{ hash: shard.hash, size: shard.size, position: shard.position }];
  }

  const range = parseRange(req.headers.range, total);
  if (req.headers.range && !range) {
    sendError(res, 416, 'range_not_satisfiable', `Range 越界，资源总长度 ${total}`);
    return;
  }
  const status = range ? 206 : 200;
  const effectiveStart = range?.start ?? 0;
  const effectiveEnd = range?.end ?? total - 1;
  const length = effectiveEnd - effectiveStart + 1;

  res.writeHead(status, {
    'content-type': 'application/octet-stream',
    'content-length': length,
    'x-object': name,
    'x-version': vid,
    'x-total-size': String(total),
    ...(range
      ? { 'content-range': `bytes ${effectiveStart}-${effectiveEnd}/${total}` }
      : { 'accept-ranges': 'bytes' }),
  });

  // 生成器：按全局字节窗口 [effectiveStart, effectiveEnd] 依次产出各 blob 的对应区间。
  // blob 以只读流逐个打开；慢客户端反压会暂停读取，但连接（及租约）保持有效。
  const generator = async function* () {
    let cursor = 0;
    for (const part of ordered) {
      const partStart = cursor;
      const partEnd = cursor + part.size - 1;
      cursor = partEnd + 1;
      if (partEnd < effectiveStart || partStart > effectiveEnd) continue;
      const skip = Math.max(0, effectiveStart - partStart);
      const take =
        Math.min(part.size - skip, effectiveEnd - Math.max(partStart, effectiveStart) + 1);
      if (take <= 0) continue;
      const rs = store.openBlob(part.hash, skip);
      let remaining = take;
      for await (const chunk of rs) {
        const buf = chunk as Buffer;
        if (remaining <= 0) break;
        if (buf.length <= remaining) {
          remaining -= buf.length;
          yield buf;
        } else {
          yield buf.subarray(0, remaining);
          remaining = 0;
        }
      }
      rs.destroy();
    }
  };

  await pipeline(Readable.from(generator()), res);
}

export async function listen(
  app: ReturnType<typeof createServer>,
  port: number,
  host = '127.0.0.1',
): Promise<number> {
  await new Promise<void>((resolve) => app.listen(port, host, () => resolve()));
  return (app.address() as AddressInfo).port;
}

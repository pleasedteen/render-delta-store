import { createHash, type Hash } from 'node:crypto';
import { createReadStream, createWriteStream, rmSync, type ReadStream } from 'node:fs';
import { readdir, rename, rm, stat } from 'node:fs/promises';
import type { IncomingMessage } from 'node:http';
import { join, resolve } from 'node:path';
import { Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import {
  badRequest,
  conflict,
  gone,
  notFound,
} from './util/errors.js';
import { atomicWriteFileSync, ensureDir, exists, fsyncDir } from './util/fsutil.js';
import { randomId, sha256Hex } from './util/hash.js';
import { KeyedMutex } from './util/mutex.js';
import { readJsonFile, writeJsonFile } from './util/jsonfile.js';
import type {
  BlobStat,
  Lease,
  ObjectIndex,
  ObjectView,
  ReclaimResult,
  Session,
  SessionView,
  ShardEntry,
  VersionManifest,
  VersionState,
} from './types.js';

/** 测试用故障注入点：在指定动作后强制终止进程（SIGKILL 语义，不执行 flush）。 */
export type CrashPoint =
  | 'after_blob_write'
  | 'before_session_update'
  | 'before_index_swap'
  | 'before_session_publish'
  | 'after_session_publish';

function crashAt(point: CrashPoint): void {
  if (process.env.FAULT_CRASH === point) {
    process.kill(process.pid, 'SIGKILL');
  }
}

const NAME_RE = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

function validateName(name: string): void {
  if (!NAME_RE.test(name)) {
    throw badRequest(`非法对象名: ${JSON.stringify(name)}`);
  }
}

export interface SessionInit {
  object: string;
  mode: 'full' | 'delta';
  shardSize: number;
  shardCount: number;
  totalSize?: number;
  positions?: number[];
  baseVid?: string;
}

/** 稳定的 JSON 序列化：键排序、无多余空白，保证版本 id 跨进程/重启可复现。 */
function canonicalJson(value: unknown): string {
  return JSON.stringify(sortDeep(value));
}
function sortDeep(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortDeep);
  if (value && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const k of Object.keys(value as Record<string, unknown>).sort()) {
      out[k] = sortDeep((value as Record<string, unknown>)[k]);
    }
    return out;
  }
  return value;
}

export class ObjectStore {
  readonly root: string;
  private readonly dirs: {
    blobs: string;
    sessions: string;
    versions: string;
    objects: string;
    leases: string;
    tmp: string;
  };
  private readonly sessions = new Map<string, Session>();
  private readonly objects = new Map<string, ObjectIndex>();
  private readonly leases = new Map<string, Lease>();
  /**
   * 进行中的下载响应钉住的 blob：hash -> 引用计数。仅存内存。
   * 响应结束或中断即释放，与租约正交：即使授权下载的租约中途释放/过期，
   * 已开始的响应也能读完；暂停的 Range 只占用其字节区间实际需要的分片。
   */
  private readonly shardHolds = new Map<string, number>();
  private readonly sessionLocks = new KeyedMutex();
  private readonly objectLocks = new KeyedMutex();

  constructor(root: string) {
    this.root = resolve(root);
    this.dirs = {
      blobs: join(this.root, 'blobs'),
      sessions: join(this.root, 'sessions'),
      versions: join(this.root, 'versions'),
      objects: join(this.root, 'objects'),
      leases: join(this.root, 'leases'),
      tmp: join(this.root, 'tmp'),
    };
  }

  // ---------------------------------------------------------------- 路径
  blobPath(hash: string): string {
    return join(this.dirs.blobs, hash.slice(0, 2), hash);
  }
  private sessionPath(sid: string) {
    return join(this.dirs.sessions, `${sid}.json`);
  }
  private versionPath(vid: string) {
    return join(this.dirs.versions, `${vid}.json`);
  }
  private objectPath(name: string) {
    return join(this.dirs.objects, `${name}.json`);
  }
  private leasePath(lid: string) {
    return join(this.dirs.leases, `${lid}.json`);
  }
  private tmpPath(tag: string) {
    return join(
      this.dirs.tmp,
      `${tag}.${process.pid}.${Date.now()}.${Math.random().toString(36).slice(2)}`,
    );
  }

  // ---------------------------------------------------------------- 启动/崩溃恢复
  async init(): Promise<void> {
    for (const d of Object.values(this.dirs)) ensureDir(d);

    // 1. 清理临时写入残留（含被强制终止的上传、半成品）。
    const leftovers = await readdir(this.dirs.tmp).catch(() => [] as string[]);
    await Promise.all(leftovers.map((f) => rm(join(this.dirs.tmp, f), { force: true })));

    // 2. 加载租约（过期则删除）。
    for (const f of await readdir(this.dirs.leases)) {
      if (!f.endsWith('.json')) continue;
      const lease = readJsonFile<Lease>(join(this.dirs.leases, f));
      if (lease.expiresAt !== null && lease.expiresAt < Date.now()) {
        await rm(this.leasePath(lease.lid), { force: true });
        continue;
      }
      this.leases.set(lease.lid, lease);
    }

    // 3. 加载对象索引。
    for (const f of await readdir(this.dirs.objects)) {
      if (!f.endsWith('.json')) continue;
      const idx = readJsonFile<ObjectIndex>(join(this.dirs.objects, f));
      this.objects.set(idx.object, idx);
    }

    // 4. 加载会话。
    for (const f of await readdir(this.dirs.sessions)) {
      if (!f.endsWith('.json')) continue;
      const s = readJsonFile<Session>(join(this.dirs.sessions, f));
      this.sessions.set(s.sid, s);
    }

    // 5. 加载版本清单。
    const manifests = new Map<string, VersionManifest>();
    for (const f of await readdir(this.dirs.versions)) {
      if (!f.endsWith('.json')) continue;
      const m = readJsonFile<VersionManifest>(join(this.dirs.versions, f));
      manifests.set(m.vid, m);
    }

    // 6. 崩溃恢复。已发布会话分两种情况：
    //    a) 清单已在磁盘：发布流程已走完关键落盘，完全信任磁盘上的清单与索引，
    //       只在索引恰好缺少该版本时补登记，绝不重算/覆盖 currentVid；
    //    b) 清单缺失：崩溃发生在“写清单之后、标记会话之前”不可能（顺序相反），
    //       实际只可能是清单 rename 未完成而会话已标记的极端窗口，此时按会话
    //       确定性重建清单，重放 vid 必须与 publishedVid 一致。
    for (const s of this.sessions.values()) {
      if (s.status !== 'published' || !s.publishedVid) continue;
      const existing = manifests.get(s.publishedVid);
      if (existing) {
        const idx = this.objects.get(s.object) ?? this.freshIndex(s.object);
        if (!idx.versions.some((v) => v.vid === existing.vid)) {
          idx.versions.push({
            vid: existing.vid,
            state: existing.state,
            createdAt: existing.createdAt,
          });
          idx.currentVid = existing.vid;
          idx.updatedAt = Date.now();
          writeJsonFile(this.objectPath(s.object), idx);
          this.objects.set(s.object, idx);
        }
        continue;
      }

      const manifest = this.buildManifest(s);
      if (manifest.vid !== s.publishedVid) {
        throw new Error(`数据损坏：会话 ${s.sid} 的版本 id 重放不一致`);
      }
      writeJsonFile(this.versionPath(manifest.vid), manifest);
      manifests.set(manifest.vid, manifest);
      const idx = this.objects.get(s.object) ?? this.freshIndex(s.object);
      if (!idx.versions.some((v) => v.vid === manifest.vid)) {
        idx.versions.push({
          vid: manifest.vid,
          state: manifest.state,
          createdAt: manifest.createdAt,
        });
      }
      idx.currentVid = manifest.vid;
      idx.updatedAt = Date.now();
      writeJsonFile(this.objectPath(s.object), idx);
      this.objects.set(s.object, idx);
    }

    // 7. 以版本清单中的 state 为准校正索引（回收可能在索引落盘前崩溃）。
    for (const idx of this.objects.values()) {
      let changed = false;
      for (const v of idx.versions) {
        const m = manifests.get(v.vid);
        if (m && m.state !== v.state) {
          v.state = m.state;
          changed = true;
        }
      }
      if (changed) writeJsonFile(this.objectPath(idx.object), idx);
    }
  }

  private freshIndex(name: string): ObjectIndex {
    return { object: name, currentVid: null, versions: [], updatedAt: Date.now() };
  }

  // ---------------------------------------------------------------- 会话管理
  async createSession(input: SessionInit): Promise<SessionView> {
    validateName(input.object);
    if (!Number.isInteger(input.shardSize) || input.shardSize <= 0) {
      throw badRequest('shardSize 必须为正整数');
    }
    if (!Number.isInteger(input.shardCount) || input.shardCount <= 0) {
      throw badRequest('shardCount 必须为正整数');
    }
    const sid = randomId('sess');
    const now = Date.now();

    if (input.mode === 'full') {
      const positions = input.positions ?? [...Array(input.shardCount).keys()];
      this.validatePositions(positions, input.shardCount);
      const session: Session = {
        sid,
        object: input.object,
        shardSize: input.shardSize,
        shardCount: input.shardCount,
        totalSize: input.totalSize ?? input.shardSize * input.shardCount,
        mode: 'full',
        base: null,
        positions,
        uploaded: {},
        status: 'open',
        publishedVid: null,
        createdAt: now,
        updatedAt: now,
      };
      this.sessions.set(sid, session);
      writeJsonFile(this.sessionPath(sid), session);
      return this.sessionView(session);
    }

    // 差量会话
    if (!input.baseVid) throw badRequest('差量会话必须提供 baseVid');
    if (!input.positions || input.positions.length === 0) {
      throw badRequest('差量会话必须提供至少一个替换位置 positions');
    }
    this.validatePositions(input.positions, input.shardCount);
    const base = this.getManifest(input.baseVid);
    if (base.state === 'reclaimed') {
      throw gone(`基版 ${input.baseVid} 已回收，不能再创建差量会话`);
    }
    if (base.object !== input.object) throw badRequest('基版不属于该对象');
    if (base.shardSize !== input.shardSize || base.shardCount !== input.shardCount) {
      throw badRequest('分片规格与基版不一致');
    }
    const session: Session = {
      sid,
      object: input.object,
      shardSize: input.shardSize,
      shardCount: input.shardCount,
      totalSize: base.totalSize,
      mode: 'delta',
      // 关键：固化基版清单快照。此后即使基版被回收，会话仍可独立组装发布。
      base: { vid: base.vid, shards: base.shards.map((x) => ({ ...x })) },
      positions: [...new Set(input.positions)].sort((a, b) => a - b),
      uploaded: {},
      status: 'open',
      publishedVid: null,
      createdAt: now,
      updatedAt: now,
    };
    this.sessions.set(sid, session);
    writeJsonFile(this.sessionPath(sid), session);
    return this.sessionView(session);
  }

  private validatePositions(positions: number[], shardCount: number): void {
    for (const p of positions) {
      if (!Number.isInteger(p) || p < 0 || p >= shardCount) {
        throw badRequest(`分片位置越界: ${p}`);
      }
    }
    if (new Set(positions).size !== positions.length) {
      throw badRequest('positions 存在重复位置');
    }
  }

  getSessionView(sid: string): SessionView {
    return this.sessionView(this.requireSession(sid));
  }

  private requireSession(sid: string): Session {
    const s = this.sessions.get(sid);
    if (!s) throw notFound(`会话不存在: ${sid}`);
    return s;
  }

  private sessionView(s: Session): SessionView {
    const confirmed = Object.keys(s.uploaded)
      .map(Number)
      .sort((a, b) => a - b);
    const pending = s.positions.filter((p) => !(p in s.uploaded));
    return {
      sid: s.sid,
      object: s.object,
      mode: s.mode,
      shardSize: s.shardSize,
      shardCount: s.shardCount,
      positions: [...s.positions],
      confirmed,
      pending,
      status: s.status,
      baseVid: s.base?.vid ?? null,
      publishedVid: s.publishedVid,
    };
  }

  // ---------------------------------------------------------------- 分片上传（确认后不可变）
  async putShard(
    sid: string,
    position: number,
    stream: IncomingMessage,
    headers: { hash?: string; contentLength?: number } = {},
  ): Promise<{ shard: ShardEntry; idempotent: boolean }> {
    return this.sessionLocks.run(sid, async () => {
      const session = this.requireSession(sid);
      if (session.status === 'published') {
        throw conflict('会话已发布，不能再上传分片');
      }
      if (!Number.isInteger(position) || !session.positions.includes(position)) {
        throw badRequest(`位置 ${position} 不属于该会话的替换集合`);
      }

      const existing = session.uploaded[position];
      if (existing) {
        // 已确认字节不可改写：原样重试直接成功；声明不同 hash 则冲突。
        if (headers.hash && headers.hash !== existing.hash) {
          throw conflict(`位置 ${position} 已确认，字节不可改写`);
        }
        return { shard: existing, idempotent: true };
      }

      // 管道：网络流 -> hash 计算 Transform -> 临时文件
      const tmp = this.tmpPath(`shard-${sid}-${position}`);
      const hasher: Hash = createHash('sha256');
      let size = 0;
      const hashTap = new Transform({
        transform(chunk: Buffer, _enc, cb) {
          size += chunk.length;
          hasher.update(chunk);
          cb(null, chunk);
        },
      });
      await pipeline(stream, hashTap, createWriteStream(tmp));
      const hash = hasher.digest('hex');

      if (headers.hash && headers.hash !== hash) {
        await rm(tmp, { force: true });
        throw conflict(`内容 hash 与 X-Shard-Hash 声明不符（实际 ${hash}）`);
      }
      if (headers.contentLength !== undefined && headers.contentLength !== size) {
        await rm(tmp, { force: true });
        throw conflict(`声明长度 ${headers.contentLength} 与实际 ${size} 不符`);
      }
      if (size === 0) {
        await rm(tmp, { force: true });
        throw badRequest('分片内容为空');
      }

      const entry: ShardEntry = { position, size, hash };
      const target = this.blobPath(hash);

      if (exists(target)) {
        // 内容寻址去重：相同 blob 已存在，直接丢弃临时文件，零额外占用。
        await rm(tmp, { force: true });
      } else {
        ensureDir(join(this.dirs.blobs, hash.slice(0, 2)));
        // rename 原子发布 blob；崩溃只会留下 tmp 残留（启动时清理），绝不会出现半 blob。
        await rename(tmp, target);
        fsyncDir(join(this.dirs.blobs, hash.slice(0, 2)));
        crashAt('after_blob_write');
      }

      // 确认进会话元数据（原子写）。崩溃在此之前：blob 可能成为孤儿（回收清理），
      // 会话未确认，客户端重试即可。
      crashAt('before_session_update');
      session.uploaded[position] = entry;
      session.updatedAt = Date.now();
      writeJsonFile(this.sessionPath(session.sid), session);

      return { shard: entry, idempotent: false };
    });
  }

  // ---------------------------------------------------------------- 发布
  async publish(sid: string): Promise<{ vid: string; object: string; idempotent: boolean }> {
    const session = this.requireSession(sid);
    return this.objectLocks.run(session.object, async () => {
      // 已发布过：无论该版本当前处于什么状态（即使已回收），都返回第一次的版本标识。
      if (session.status === 'published' && session.publishedVid) {
        return { vid: session.publishedVid, object: session.object, idempotent: true };
      }

      const missing = session.positions.filter((p) => !(p in session.uploaded));
      if (missing.length > 0) {
        throw conflict(`分片未传齐，缺失位置: ${missing.join(', ')}`);
      }

      const manifest = this.buildManifest(session);
      crashAt('before_session_publish');

      // 1) 版本清单先落盘（若因去重已存在则跳过）。
      if (!exists(this.versionPath(manifest.vid))) {
        writeJsonFile(this.versionPath(manifest.vid), manifest);
      }

      // 2) 切换对象索引：旧 current -> superseded，新版本 -> current。单文件原子写，
      //    保证“当前版本只能是完整旧版或完整新版”。
      crashAt('before_index_swap');
      const idx = this.objects.get(session.object) ?? this.freshIndex(session.object);
      if (idx.currentVid) {
        const old = idx.versions.find((v) => v.vid === idx.currentVid);
        if (old) old.state = 'superseded';
        const oldManifest = this.tryGetManifest(idx.currentVid);
        if (oldManifest && oldManifest.state === 'current') {
          oldManifest.state = 'superseded';
          writeJsonFile(this.versionPath(oldManifest.vid), oldManifest);
        }
      }
      if (!idx.versions.some((v) => v.vid === manifest.vid)) {
        idx.versions.push({
          vid: manifest.vid,
          state: 'current',
          createdAt: manifest.createdAt,
        });
      } else {
        const v = idx.versions.find((x) => x.vid === manifest.vid)!;
        v.state = 'current';
      }
      idx.currentVid = manifest.vid;
      idx.updatedAt = Date.now();
      writeJsonFile(this.objectPath(session.object), idx);
      this.objects.set(session.object, idx);

      // 3) 最后标记会话已发布并记录 vid。崩溃在 2 之后、3 之前：启动恢复会依据
      //    确定性 vid 补齐索引，且重复发布仍返回同一 vid。
      session.status = 'published';
      session.publishedVid = manifest.vid;
      session.updatedAt = Date.now();
      writeJsonFile(this.sessionPath(session.sid), session);

      // 故障点：所有落盘已完成、HTTP 响应尚未返回时崩溃，模拟“响应丢失”窗口。
      crashAt('after_session_publish');

      return { vid: manifest.vid, object: session.object, idempotent: false };
    });
  }

  /** 由“基版未替换分片 + 本次上传新分片”确定性组装整包清单。 */
  private buildManifest(session: Session): VersionManifest {
    const byPos = new Map<number, ShardEntry>();

    if (session.mode === 'delta' && session.base) {
      for (const shard of session.base.shards) byPos.set(shard.position, { ...shard });
    }
    for (const position of session.positions) {
      const entry = session.uploaded[position];
      if (!entry) throw conflict(`分片未传齐，缺失位置: ${position}`);
      byPos.set(position, { ...entry });
    }

    const shards = [...byPos.values()].sort((a, b) => a.position - b.position);
    if (shards.length !== session.shardCount) {
      throw conflict(
        `组装失败：期望 ${session.shardCount} 片，实际 ${shards.length} 片`,
      );
    }
    for (let i = 0; i < shards.length; i++) {
      if (shards[i]!.position !== i) throw conflict(`组装失败：位置 ${i} 缺失`);
    }

    const totalSize = shards.reduce((sum, s) => sum + s.size, 0);
    // vid 只由对象名与分片内容（位置/大小/hash）决定，不含时间戳等易变字段，
    // 保证崩溃恢复重放与首次发布得到完全相同的版本标识。
    const hashBase = {
      object: session.object,
      shardSize: session.shardSize,
      shardCount: session.shardCount,
      totalSize,
      shards,
    };
    const vid = `v-${sha256Hex(canonicalJson(hashBase)).slice(0, 32)}`;
    return {
      vid,
      object: session.object,
      shardSize: session.shardSize,
      shardCount: session.shardCount,
      totalSize,
      shards,
      createdAt: Date.now(),
      state: 'current' as VersionState,
    };
  }

  // ---------------------------------------------------------------- 查询
  getManifest(vid: string): VersionManifest {
    const m = this.tryGetManifest(vid);
    if (!m) throw notFound(`版本不存在: ${vid}`);
    return m;
  }
  private tryGetManifest(vid: string): VersionManifest | null {
    const path = this.versionPath(vid);
    if (!exists(path)) return null;
    return readJsonFile<VersionManifest>(path);
  }

  getObjectView(name: string): ObjectView {
    validateName(name);
    const idx = this.objects.get(name);
    if (!idx) throw notFound(`对象不存在: ${name}`);
    return {
      object: name,
      currentVid: idx.currentVid,
      versions: idx.versions.map((v) => ({
        vid: v.vid,
        state: v.state,
        createdAt: v.createdAt,
        downloadable: this.isDownloadable(v.vid, v.state),
      })),
    };
  }

  /** 有有效租约时，即使版本已回收也仍可读完。 */
  private isDownloadable(vid: string, state: VersionState): boolean {
    if (state !== 'reclaimed') return true;
    return this.activeLeaseVids().has(vid);
  }

  private activeLeaseVids(): Set<string> {
    this.dropExpiredLeases();
    return new Set([...this.leases.values()].map((l) => l.vid));
  }

  private dropExpiredLeases(): void {
    const now = Date.now();
    for (const [lid, lease] of this.leases) {
      if (lease.expiresAt !== null && lease.expiresAt < now) {
        this.leases.delete(lid);
        this.removeLeaseFile(lid);
      }
    }
  }

  listObjects(): Array<{ object: string; currentVid: string | null }> {
    return [...this.objects.values()].map((idx) => ({
      object: idx.object,
      currentVid: idx.currentVid,
    }));
  }

  // ---------------------------------------------------------------- 下载与租约
  createLease(name: string, vid: string, ttlSeconds: number | null): Lease {
    validateName(name);
    const idx = this.objects.get(name);
    const entry = idx?.versions.find((v) => v.vid === vid);
    if (!idx || !entry) {
      throw notFound(`版本不存在: ${vid}`);
    }
    // 旧版撤下（已回收）后停止签发新租约；此前签发的租约在有效期内仍可用。
    if (entry.state === 'reclaimed') {
      throw gone(`版本 ${vid} 已回收，不再签发新租约`);
    }
    if (ttlSeconds !== null && (!Number.isFinite(ttlSeconds) || ttlSeconds <= 0)) {
      throw badRequest('ttlSeconds 必须为正数或 null');
    }
    const lease: Lease = {
      lid: randomId('lease'),
      object: name,
      vid,
      createdAt: Date.now(),
      expiresAt: ttlSeconds === null ? null : Date.now() + ttlSeconds * 1000,
    };
    this.leases.set(lease.lid, lease);
    writeJsonFile(this.leasePath(lease.lid), lease);
    return lease;
  }

  /**
   * 响应级分片占用：下载响应开始时登记其字节区间实际需要的 blob，
   * 返回的释放函数在响应结束（或连接中断）时调用。与租约正交——
   * 即使授权本次下载的租约中途被释放或过期，已开始的响应也能读完。
   * 与 requireDownloadable 在同一同步窗口内登记，回收无法穿插进来。
   */
  acquireShardHolds(hashes: Iterable<string>): () => void {
    const held: string[] = [];
    for (const h of hashes) {
      this.shardHolds.set(h, (this.shardHolds.get(h) ?? 0) + 1);
      held.push(h);
    }
    let released = false;
    return () => {
      if (released) return;
      released = true;
      for (const h of held) {
        const n = this.shardHolds.get(h);
        if (n === undefined) continue;
        if (n <= 1) this.shardHolds.delete(h);
        else this.shardHolds.set(h, n - 1);
      }
    };
  }

  /** 当前被进行中响应钉住的 blob 集合（回收与测试共用）。 */
  heldBlobHashes(): Set<string> {
    return new Set(this.shardHolds.keys());
  }

  private removeLeaseFile(lid: string): void {
    rmSync(this.leasePath(lid), { force: true });
  }

  releaseLease(lid: string): void {
    const lease = this.leases.get(lid);
    if (!lease) throw notFound(`租约不存在: ${lid}`);
    this.leases.delete(lid);
    this.removeLeaseFile(lid);
  }

  /** 校验下载权限：版本须存在；已回收版本只有持有效租约才可继续读。 */
  requireDownloadable(vid: string, leaseId: string | null): VersionManifest {
    const manifest = this.getManifest(vid);
    if (manifest.state !== 'reclaimed') return manifest;
    if (leaseId) {
      const lease = this.leases.get(leaseId);
      if (lease && lease.vid === vid) {
        if (lease.expiresAt === null || lease.expiresAt >= Date.now()) return manifest;
      }
      throw gone('租约无效或已过期');
    }
    throw gone(`版本 ${vid} 已回收`);
  }

  openBlob(hash: string, start = 0): ReadStream {
    const path = this.blobPath(hash);
    if (!exists(path)) throw gone(`分片 blob 缺失: ${hash.slice(0, 12)}`);
    return createReadStream(path, start > 0 ? { start } : {});
  }

  // ---------------------------------------------------------------- 回收
  async reclaim(name: string | null): Promise<ReclaimResult> {
    const targets = name ? [name] : [...this.objects.keys()];
    const aggregate: ReclaimResult = {
      object: name ?? '*',
      reclaimedVids: [],
      deletedBlobs: [],
      deletedTempFiles: [],
      freedBytes: 0,
    };
    for (const target of targets) {
      const r = await this.objectLocks.run(target, () => this.reclaimOne(target));
      aggregate.reclaimedVids.push(...r.reclaimedVids);
      aggregate.deletedBlobs.push(...r.deletedBlobs);
      aggregate.deletedTempFiles.push(...r.deletedTempFiles);
      aggregate.freedBytes += r.freedBytes;
    }
    return aggregate;
  }

  private async reclaimOne(name: string): Promise<ReclaimResult> {
    const result: ReclaimResult = {
      object: name,
      reclaimedVids: [],
      deletedBlobs: [],
      deletedTempFiles: [],
      freedBytes: 0,
    };
    const idx = this.objects.get(name);
    if (!idx) return result;

    // 1. 撤下所有 superseded 版本的下载入口（manifest 与索引都标记 reclaimed）。
    for (const v of idx.versions) {
      if (v.state !== 'superseded') continue;
      const m = this.tryGetManifest(v.vid);
      if (m) {
        m.state = 'reclaimed';
        writeJsonFile(this.versionPath(v.vid), m);
      }
      v.state = 'reclaimed';
      result.reclaimedVids.push(v.vid);
    }
    if (result.reclaimedVids.length > 0) {
      idx.updatedAt = Date.now();
      writeJsonFile(this.objectPath(name), idx);
    }

    // 2. 计算仍在使用的 blob 活集合。
    const live = this.computeLiveBlobs();

    // 3. 删除无引用 blob。
    const blobRoot = this.dirs.blobs;
    for (const shardDir of await readdir(blobRoot).catch(() => [] as string[])) {
      const dirPath = join(blobRoot, shardDir);
      for (const file of await readdir(dirPath).catch(() => [] as string[])) {
        const hash = file;
        if (live.has(hash)) continue;
        // 下载不持对象锁：活集合计算后可能有新响应开始，删除前再确认一次占用。
        if (this.shardHolds.has(hash)) continue;
        const full = join(dirPath, file);
        const s = await stat(full).catch(() => null);
        await rm(full, { force: true });
        result.deletedBlobs.push(hash);
        if (s) result.freedBytes += s.size;
      }
    }
    fsyncDir(blobRoot);

    // 4. 清理失败写入残留。
    for (const f of await readdir(this.dirs.tmp).catch(() => [] as string[])) {
      const full = join(this.dirs.tmp, f);
      const s = await stat(full).catch(() => null);
      await rm(full, { force: true });
      result.deletedTempFiles.push(f);
      if (s) result.freedBytes += s.size;
    }

    return result;
  }

  /**
   * 活集合来源：
   *  - 未回收版本（current/superseded）的全部分片；
   *  - 已回收但仍被有效租约（慢下载）钉住的版本分片；
   *  - 进行中下载响应钉住的分片（暂停的 Range 只占其字节区间所需 blob）；
   *  - 开放会话：已确认上传的新分片 + 差量基版中“未被本会话替换”的分片。
   *    会话明确要替换的基版分片不计入活集合（若无其他使用者即可释放）。
   */
  private computeLiveBlobs(): Set<string> {
    this.dropExpiredLeases();
    const live = new Set<string>();
    const leasedVids = new Set([...this.leases.values()].map((l) => l.vid));

    for (const idx of this.objects.values()) {
      for (const v of idx.versions) {
        if (v.state === 'reclaimed' && !leasedVids.has(v.vid)) continue;
        const m = this.tryGetManifest(v.vid);
        if (!m) continue;
        for (const shard of m.shards) live.add(shard.hash);
      }
    }

    for (const h of this.shardHolds.keys()) live.add(h);

    for (const s of this.sessions.values()) {
      if (s.status !== 'open') continue;
      for (const entry of Object.values(s.uploaded)) live.add(entry.hash);
      if (s.mode === 'delta' && s.base) {
        const replaced = new Set(s.positions);
        for (const shard of s.base.shards) {
          if (!replaced.has(shard.position)) live.add(shard.hash);
        }
      }
    }
    return live;
  }

  async blobStats(): Promise<{ blobs: BlobStat[]; totalBytes: number; count: number }> {
    const blobs: BlobStat[] = [];
    let totalBytes = 0;
    for (const shardDir of await readdir(this.dirs.blobs).catch(() => [] as string[])) {
      const dirPath = join(this.dirs.blobs, shardDir);
      for (const file of await readdir(dirPath).catch(() => [] as string[])) {
        const full = join(dirPath, file);
        const s = await stat(full).catch(() => null);
        if (s) {
          blobs.push({ hash: file, size: s.size });
          totalBytes += s.size;
        }
      }
    }
    return { blobs, totalBytes, count: blobs.length };
  }
}

// 原子写辅助集中在 fsutil；这里 re-export 供 main/测试使用。
export { atomicWriteFileSync };

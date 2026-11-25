/** 分片位置 -> 内容 hash 的映射项。 */
export interface ShardEntry {
  position: number;
  size: number;
  hash: string;
}

export type VersionState = 'current' | 'superseded' | 'reclaimed';

/** 一个已发布版本的清单。版本 id 由清单内容确定性派生，重放得到同一 id。 */
export interface VersionManifest {
  vid: string;
  object: string;
  shardSize: number;
  shardCount: number;
  totalSize: number;
  shards: ShardEntry[];
  createdAt: number;
  state: VersionState;
}

export interface SessionBase {
  vid: string;
  shards: ShardEntry[];
}

export type SessionStatus = 'open' | 'published';

/** 上传会话（全量或差量）。 */
export interface Session {
  sid: string;
  object: string;
  shardSize: number;
  shardCount: number;
  totalSize: number;
  mode: 'full' | 'delta';
  base: SessionBase | null;
  /** 本次会话要替换/写入的位置集合（有序去重）。 */
  positions: number[];
  /** 已确认上传：位置 -> 分片内容。 */
  uploaded: Record<number, ShardEntry>;
  status: SessionStatus;
  /** 发布后记录生成的版本 id；重复发布必须原样返回它。 */
  publishedVid: string | null;
  createdAt: number;
  updatedAt: number;
}

/** 对象索引：当前版本 + 全部历史版本（含已回收，保留清单用于引用判断）。 */
export interface ObjectIndex {
  object: string;
  currentVid: string | null;
  versions: Array<{ vid: string; state: VersionState; createdAt: number }>;
  updatedAt: number;
}

/** 显式下载租约，持久化以便进程重启后仍能保护慢下载。 */
export interface Lease {
  lid: string;
  object: string;
  vid: string;
  createdAt: number;
  expiresAt: number | null;
}

export interface SessionView {
  sid: string;
  object: string;
  mode: 'full' | 'delta';
  shardSize: number;
  shardCount: number;
  positions: number[];
  confirmed: number[];
  pending: number[];
  status: SessionStatus;
  baseVid: string | null;
  publishedVid: string | null;
}

export interface ObjectView {
  object: string;
  currentVid: string | null;
  versions: Array<{
    vid: string;
    state: VersionState;
    createdAt: number;
    downloadable: boolean;
  }>;
}

export interface ReclaimResult {
  object: string;
  reclaimedVids: string[];
  deletedBlobs: string[];
  deletedTempFiles: string[];
  freedBytes: number;
}

export interface BlobStat {
  hash: string;
  size: number;
}

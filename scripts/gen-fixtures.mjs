#!/usr/bin/env node
// 生成本地数据夹具：确定性的 16 片素材包清单与校验信息（默认小尺寸，便于提交进仓库）。
// 真实 128MiB / 16×8MiB 场景由测试在运行时生成，避免仓库膨胀。
import { createHash } from 'node:crypto';
import { mkdirSync, writeFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const outDir = join(here, '..', 'fixtures', 'demo-package');
mkdirSync(outDir, { recursive: true });

const SHARD_COUNT = 16;
const SHARD_SIZE = 512 * 1024; // 512 KiB × 16 = 8 MiB 夹具包

function makeShard(seed, size) {
  const buf = Buffer.alloc(size);
  let state = (seed ^ 0x9e3779b9) >>> 0;
  for (let i = 0; i < size; i += 4) {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    state = (state + Math.imul(seed + 1, i + 1)) >>> 0;
    buf.writeUInt32LE(state >>> 0, i);
  }
  return buf;
}

const shards = [];
for (let p = 0; p < SHARD_COUNT; p++) {
  const data = makeShard(p + 1, SHARD_SIZE);
  const hash = createHash('sha256').update(data).digest('hex');
  shards.push({ position: p, size: SHARD_SIZE, hash });
}
const totalSize = shards.reduce((s, x) => s + x.size, 0);
const manifest = {
  object: 'demo-package',
  shardSize: SHARD_SIZE,
  shardCount: SHARD_COUNT,
  totalSize,
  shards,
  generator: 'scripts/gen-fixtures.mjs',
  note: '确定性 PRNG 数据；测试用 makeShard(seed,size) 可逐字节复现第 position 片（seed=position+1）',
};
writeFileSync(join(outDir, 'manifest.json'), JSON.stringify(manifest, null, 2));
writeFileSync(
  join(outDir, 'README.md'),
  [
    '# demo-package 夹具',
    '',
    `- 分片数: ${SHARD_COUNT}`,
    `- 单片大小: ${SHARD_SIZE} 字节`,
    `- 整包大小: ${totalSize} 字节`,
    '- 内容: 确定性伪随机（PRNG），无真实素材版权问题',
    '- 重新生成: `npm run fixtures`',
    '',
  ].join('\n'),
);
console.log(`夹具已生成: ${outDir}/manifest.json (${totalSize} 字节, ${SHARD_COUNT} 片)`);

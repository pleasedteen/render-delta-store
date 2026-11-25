import {
  closeSync,
  existsSync,
  fsyncSync,
  mkdirSync,
  openSync,
  renameSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { dirname } from 'node:path';

export function ensureDir(dir: string): void {
  mkdirSync(dir, { recursive: true });
}

export function exists(path: string): boolean {
  return existsSync(path);
}

/** 尽力 fsync 目录，保证其中的 rename/unlink 元数据落盘。某些文件系统不支持则忽略。 */
export function fsyncDir(dir: string): void {
  try {
    const fd = openSync(dir, 'r');
    try {
      fsyncSync(fd);
    } finally {
      closeSync(fd);
    }
  } catch {
    /* 某些文件系统（如部分 overlay/tmpfs）不支持目录 fsync，忽略 */
  }
}

/**
 * 崩溃安全的原子写：先写同目录临时文件，fsync 后 rename 到目标，再 fsync 目录。
 * 任何时刻崩溃，目标位置要么是旧完整内容，要么是新完整内容，绝不会出现半成品。
 */
export function atomicWriteFileSync(target: string, data: Buffer | string): void {
  ensureDir(dirname(target));
  const tmp = `${target}.tmp.${process.pid}.${Date.now()}.${Math.random().toString(36).slice(2)}`;
  writeFileSync(tmp, data);
  const fd = openSync(tmp, 'r');
  try {
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
  renameSync(tmp, target);
  fsyncDir(dirname(target));
}

export function rmIfExists(path: string): void {
  try {
    rmSync(path, { force: true, recursive: true });
  } catch {
    /* 不存在即达成目标 */
  }
}

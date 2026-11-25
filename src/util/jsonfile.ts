import { readFileSync } from 'node:fs';
import { atomicWriteFileSync, ensureDir, exists, rmIfExists } from './fsutil.js';

export function readJsonFile<T>(path: string): T {
  return JSON.parse(readFileSync(path, 'utf8')) as T;
}

export function writeJsonFile(path: string, value: unknown): void {
  atomicWriteFileSync(path, JSON.stringify(value));
}

export function readJsonIfExists<T>(path: string): T | null {
  if (!exists(path)) return null;
  return readJsonFile<T>(path);
}

export function removeJsonIfExists(path: string): void {
  rmIfExists(path);
}

export function ensureJsonDir(path: string): void {
  ensureDir(path);
}

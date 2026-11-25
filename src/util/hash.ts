import { createHash, randomBytes } from 'node:crypto';

export function sha256Hex(data: Buffer | string): string {
  return createHash('sha256').update(data).digest('hex');
}

export function randomId(prefix: string): string {
  return `${prefix}_${randomBytes(12).toString('hex')}`;
}

import { resolve } from 'node:path';
import { ObjectStore } from './store.js';
import { createApp, listen } from './server.js';

function parseArgs(argv: string[]): { port: number; host: string; dataDir: string } {
  const args = new Map<string, string>();
  for (let i = 2; i < argv.length; i += 2) {
    const key = argv[i];
    const val = argv[i + 1];
    if (key && val !== undefined) args.set(key, val);
  }
  const port = Number(args.get('--port') ?? process.env.PORT ?? 8080);
  const host = args.get('--host') ?? process.env.HOST ?? '127.0.0.1';
  const dataDir = resolve(
    args.get('--data') ?? process.env.DATA_DIR ?? './data',
  );
  if (!Number.isInteger(port) || port < 0 || port > 65535) throw new Error('非法端口');
  return { port, host, dataDir };
}

async function main(): Promise<void> {
  const { port, host, dataDir } = parseArgs(process.argv);
  const store = new ObjectStore(dataDir);
  await store.init();
  const app = createApp(store);
  app.on('error', (err: NodeJS.ErrnoException) => {
    if (err.code === 'EADDRINUSE') {
      // eslint-disable-next-line no-console
      console.error(`端口 ${port} 已被占用，请更换 --port 或停止占用进程`);
      process.exit(1);
    }
    throw err;
  });
  const actualPort = await listen(app, port, host);
  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify({
      msg: 'object-store listening',
      url: `http://${host}:${actualPort}`,
      dataDir,
    }),
  );

  const shutdown = (sig: string) => {
    // 正常停机：停止接收新连接，进行中的请求由 Node 默认机制收尾。
    app.close(() => {
      // eslint-disable-next-line no-console
      console.log(JSON.stringify({ msg: 'shutdown complete', sig }));
      process.exit(0);
    });
    setTimeout(() => process.exit(1), 5000).unref();
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('启动失败:', err);
  process.exit(1);
});

/** 极简互斥锁：Node 单线程事件循环下，串行化对同一对象/会话的异步临界区。 */
export class Mutex {
  private tail: Promise<unknown> = Promise.resolve();

  async run<T>(fn: () => Promise<T>): Promise<T> {
    const previous = this.tail;
    let release: () => void;
    this.tail = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;
    try {
      return await fn();
    } finally {
      release!();
    }
  }
}

export class KeyedMutex {
  private readonly locks = new Map<string, Mutex>();

  get(key: string): Mutex {
    let m = this.locks.get(key);
    if (!m) {
      m = new Mutex();
      this.locks.set(key, m);
    }
    return m;
  }

  run<T>(key: string, fn: () => Promise<T>): Promise<T> {
    return this.get(key).run(fn);
  }
}

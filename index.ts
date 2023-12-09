import {
  map,
  ReplaySubject,
  Subject,
  takeUntil,
  delayWhen,
  concatMap,
  of,
  tap,
  distinctUntilChanged,
  filter,
  Observable,
  take,
} from 'rxjs';

type QueueName = string;

interface StreamQueueItem<T> {
  id: string;
  item: T;
}

interface StreamQueueManagerOption {
  poolSize?: number;
}

enum StreamQueueStatus {
  EMPTY = 'empty',
  PENDING = 'pending',
  DESTROY = 'destroy',
}

class StreamQueue<T = unknown> {
  private readonly _queue$ = new ReplaySubject<StreamQueueItem<T>>();
  private readonly _dequeue$ = new Subject<void>();
  private readonly _dequeueStream$ = new Subject<StreamQueueItem<T>>();
  private readonly _statusChange$ = new Subject<StreamQueueStatus>();

  constructor() {
    this.registerProcessor();
  }

  get dequeueStream() {
    return this._dequeueStream$.pipe(map(({ item }) => item));
  }

  get statusChange() {
    return this._statusChange$.pipe(distinctUntilChanged());
  }

  enqueue(item: T) {
    this.enqueueWithId(this.generateUniqueId(), item);
  }

  dequeue() {
    this._dequeue$.next();
  }

  enqueueAndWaitDequeue(item: T) {
    const id = this.generateUniqueId();
    return new Observable<T>((subscriber) => {
      this._dequeueStream$
        .pipe(
          filter((streamItem) => streamItem.id === id),
          map(({ item }) => item),
          take(1)
        )
        .subscribe(subscriber);
      this.enqueueWithId(id, item);
    });
  }

  destroy() {
    this._statusChange$.next(StreamQueueStatus.DESTROY);
    this._statusChange$.complete();
    this._dequeue$.complete();
    this._dequeueStream$.complete();
  }

  private enqueueWithId(id: string, item: T) {
    this._queue$.next({
      id,
      item,
    });
  }

  private generateUniqueId() {
    return window.crypto.randomUUID();
  }

  private registerProcessor() {
    let latestIndex = 0;

    const destroy$ = this._statusChange$.pipe(
      filter((status) => status === StreamQueueStatus.DESTROY)
    );

    this._queue$
      .pipe(
        map((item, index) => {
          latestIndex = index;
          return item;
        }),
        tap(() => {
          this._statusChange$.next(StreamQueueStatus.PENDING);
        }),
        concatMap((item) => of(item).pipe(delayWhen(() => this._dequeue$))),
        map((item, index) => ({ item, index })),
        takeUntil(destroy$)
      )
      .subscribe({
        next: ({ index, item }) => {
          const status =
            latestIndex === index
              ? StreamQueueStatus.EMPTY
              : StreamQueueStatus.PENDING;
          this._statusChange$.next(status);
          this._dequeueStream$.next(item);
        },
      });
  }
}

class StreamQueueManager {
  private readonly _queuePool = new Map<QueueName, StreamQueue>();

  constructor(private readonly _options?: StreamQueueManagerOption) {}

  private get _maxPoolSize() {
    return this._options?.poolSize ?? 10;
  }

  set<T>(name: QueueName) {
    if (this._queuePool.has(name)) {
      throw new Error(`The queue ${name} is already exist.`);
    }

    if (this._queuePool.size >= this._maxPoolSize) {
      throw new Error(`The queue pool size is maximum.`);
    }

    const queue = new StreamQueue<T>();
    this._queuePool.set(name, queue);

    return queue;
  }

  get<T>(name: QueueName) {
    return this._queuePool.get(name) as StreamQueue<T> | undefined;
  }

  remove(name: QueueName) {
    const queue = this.get(name);
    this._queuePool.delete(name);
    queue?.destroy();
  }

  enqueue<T>(name: QueueName, item: T) {
    const queue = this.getOrSetQueue<T>(name);
    queue.enqueue(item);
  }

  dequeue(name: QueueName) {
    const queue = this.get(name);
    queue?.dequeue();
  }

  enqueueAndWaitDequeue<T>(name: QueueName, item: T) {
    const queue = this.getOrSetQueue<T>(name);
    return queue.enqueueAndWaitDequeue(item);
  }

  clear() {
    Array.from(this._queuePool.keys()).forEach((name) => {
      this.remove(name);
    });
  }

  getDequeueStream<T>(name: QueueName) {
    const queue = this.getOrSetQueue<T>(name);
    return queue.dequeueStream;
  }

  getStatusChange(name: QueueName) {
    const queue = this.getOrSetQueue(name);
    return queue.statusChange;
  }

  private getOrSetQueue<T>(name: QueueName) {
    const queue = this.get<T>(name);

    if (!queue) {
      return this.set<T>(name);
    }

    return queue;
  }
}

declare module "@surikat/job-queue" {
  import { EventEmitter } from "events";

  class JobQueue<T = unknown> extends EventEmitter {
    constructor(processor: (item: T, done: (err?: Error) => void) => void);
    add(item: T): void;
    pause(): void;
    resume(): void;
    clear(): void;
  }

  export = JobQueue;
}

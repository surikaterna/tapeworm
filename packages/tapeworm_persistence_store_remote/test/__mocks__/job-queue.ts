import { EventEmitter } from "events";

/**
 * Minimal mock of @surikat/job-queue for testing.
 * Processes items sequentially, emits 'empty' when done, 'error' on failure.
 */
class JobQueue<T = unknown> extends EventEmitter {
  private _processor: (item: T, done: (err?: Error) => void) => void;
  private _queue: T[];
  private _paused: boolean;
  private _processing: boolean;

  constructor(processor: (item: T, done: (err?: Error) => void) => void) {
    super();
    this._processor = processor;
    this._queue = [];
    this._paused = false;
    this._processing = false;
  }

  add(item: T): void {
    this._queue.push(item);
    if (!this._paused) {
      this._process();
    }
  }

  pause(): void {
    this._paused = true;
  }

  resume(): void {
    this._paused = false;
    this._process();
  }

  clear(): void {
    this._queue = [];
  }

  private _process(): void {
    if (this._processing || this._paused) return;
    if (this._queue.length === 0) {
      this.emit("empty");
      return;
    }
    this._processing = true;
    var item = this._queue.shift()!;
    var self = this;
    this._processor(item, function (err?: Error) {
      self._processing = false;
      if (err) {
        self.emit("error", err);
      } else {
        self._process();
      }
    });
  }
}

export default JobQueue;

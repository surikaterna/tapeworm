import Promise from "bluebird";
import { EventEmitter } from "events";

class Handle extends EventEmitter {
  stop: () => void;
  synced: Promise<unknown>;

  constructor(stop: () => void) {
    super();
    this.stop = stop;
    this.synced = new Promise((resolve) => {
      this.once("insync", function (commits: unknown) {
        resolve(commits);
      });
    });
  }
}

export default Handle;

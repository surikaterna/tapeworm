import { describe, it, expect } from "vitest";
import Promise from "bluebird";
import { v4 as uuid } from "uuid";

import EventStore, { EventStream } from "tapeworm";

import IdbPersistenceStore from "..";

import getDb from "./util";

describe("event_stream", () => {
  describe("#openStream", () => {
    it("should return 0 commits for new stream", async () => {
      var es = new EventStore(new IdbPersistenceStore(getDb()));
      var partition = await es.openPartition("location");
      var stream = await partition.openStream("1");
      expect(stream.getCommittedEvents().length).toBe(0);
    });
  });
  describe("#commit", () => {
    it("should do nothing if nothing has been appended", async () => {
      var es = new EventStore(new IdbPersistenceStore(getDb()));
      var partition = await es.openPartition("location");
      var stream = await partition.openStream("1");
      stream.commit(uuid());
      expect(stream.getCommittedEvents().length).toBe(0);
    });

    it("should call commit on partition", () => {
      var mockPartition = {
        called: false,
        append: function (commit: unknown, callback?: unknown) {
          this.called = true;
          return Promise.resolve().nodeify(
            callback as (() => void) | undefined,
          );
        },
        _queryStream: function (streamId: string, callback?: unknown) {
          return Promise.resolve([]).nodeify(
            callback as (() => void) | undefined,
          );
        },
      };
      var stream = new EventStream(mockPartition, "11");
      stream.append({ event: "123" });
      stream.commit(uuid());

      expect(mockPartition.called).toBe(true);
    });
    it("should keep track of uncommitted events", async () => {
      var es = new EventStore(new IdbPersistenceStore(getDb()));
      var partition = await es.openPartition("location");
      var stream = await partition.openStream("1");
      stream.append({ event: "123" });
      expect(stream.getUncommittedEvents().length).toBe(1);
    });
    it("should move uncommitted events to committed on commit", async () => {
      var es = new EventStore(new IdbPersistenceStore(getDb()));
      var partition = await es.openPartition("location");
      var stream = await partition.openStream("1");
      stream.append({ event: "123" });
      await stream.commit(uuid());
      expect(stream.getUncommittedEvents().length).toBe(0);
      expect(stream.getCommittedEvents().length).toBe(1);
    });
    it("two events becomes one commit", async () => {
      var es = new EventStore(new IdbPersistenceStore(getDb()));
      var partition = await es.openPartition("location");
      var stream = await partition.openStream("1");
      stream.append({ event: "123" });
      stream.append({ event: "999" });
      await stream.commit(uuid());
      expect(stream.getCommittedEvents().length).toBe(2);
      expect(stream._commitSequence).toBe(0);
    });
  });
});

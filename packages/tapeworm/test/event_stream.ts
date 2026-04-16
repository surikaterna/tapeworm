import Promise from "bluebird";
import { v4 as uuid } from "uuid";
import { describe, expect, it } from "vitest";
import EventStore from "..";
import EventStream from "../src/event_stream";

describe("event_stream", () => {
  describe("#openStream", () => {
    it("should return 0 commits for new stream", async () => {
      const es = new EventStore();
      const stream = await es.openPartition("location").call("openStream", "1");
      expect(stream.getCommittedEvents()).toHaveLength(0);
    });
  });

  describe("#commit", () => {
    it("should do nothing if nothing has been appended", async () => {
      const es = new EventStore();
      const stream = await es.openPartition("location").call("openStream", "1");
      stream.commit(uuid());
      expect(stream.getCommittedEvents()).toHaveLength(0);
    });

    it("should call commit on partition", () => {
      let called = false;
      const mockPartition = {
        append: (commit, callback) => {
          called = true;
          return Promise.resolve().nodeify(callback);
        },
        _queryStream: (streamId, callback) => {
          return Promise.resolve([]).nodeify(callback);
        },
      };

      const stream = new EventStream(mockPartition, "11");
      stream.append({ event: "123" });
      stream.commit(uuid());

      expect(called).toBe(true);
    });

    it("should keep track of uncommitted events", async () => {
      const es = new EventStore();
      const stream = await es.openPartition("location").call("openStream", "1");

      stream.append({ event: "123" });
      expect(stream.getUncommittedEvents()).toHaveLength(1);
    });

    it("should move uncommitted events to committed on commit", async () => {
      const es = new EventStore();
      const stream = await es.openPartition("location").call("openStream", "1");

      stream.append({ event: "123" });
      await stream.commit(uuid());

      expect(stream.getUncommittedEvents()).toHaveLength(0);
      expect(stream.getCommittedEvents()).toHaveLength(1);
    });

    it("two events becomes one commit", async () => {
      const es = new EventStore();
      const stream = await es.openPartition("location").call("openStream", "1");

      stream.append({ event: "123" });
      stream.append({ event: "999" });
      await stream.commit(uuid());

      expect(stream.getCommittedEvents()).toHaveLength(2);
      expect(stream._commitSequence).toBe(0);
    });

    it("two commits gets increasing commit sequence", async () => {
      const es = new EventStore();
      const stream = await es.openPartition("location").call("openStream", "1");

      stream.append({ event: "123" });
      stream.append({ event: "999" });
      await stream.commit(uuid());

      stream.append({ event: "666" });
      stream.append({ event: "777" });
      await stream.commit(uuid());

      expect(stream.getCommittedEvents()).toHaveLength(4);
      expect(stream._commitSequence).toBe(1);
    });

    it("event stream writeOnly", async () => {
      const es = new EventStore();
      const partition = await es.openPartition("location");
      const stream = await partition.openStream("1", true);

      stream.append({ event: "123" });
      await stream.commit(uuid());

      expect(stream._commitSequence).toBe(0);
      stream.append({ event: "666" });
      stream.append({ event: "777" });
      await stream.commit(uuid());

      expect(stream._commitSequence).toBe(1);
    });

    it("committed events should have increasing version", async () => {
      const es = new EventStore();
      const stream = await es.openPartition("location").call("openStream", "1");

      stream.append({ event: "123" });
      stream.append({ event: "999" });
      await stream.commit(uuid());

      stream.append({ event: "666" });
      stream.append({ event: "777" });
      await stream.commit(uuid());

      expect(stream.getCommittedEvents()[3].version).toBe(3);
      expect(stream._version).toBe(4);
    });

    it("committed events should have increasing version (writeOnly)", async () => {
      const es = new EventStore();
      const partition = await es.openPartition("location");
      const stream = await partition.openStream("1", true);

      stream.append({ event: "123" });
      stream.append({ event: "999" });
      await stream.commit(uuid());

      stream.append({ event: "666" });
      stream.append({ event: "777" });
      await stream.commit(uuid());

      expect(stream._version).toBe(4);
    });

    it("published events should have increasing version", async () => {
      let commitCount = 0;

      const es = new EventStore(null, (commit) => {
        expect(commit.events[0]).toHaveProperty("version");
        commitCount++;
      });

      const stream = await es.openPartition("location").call("openStream", "1");

      stream.append({ event: "123" });
      stream.append({ event: "999" });
      await stream.commit(uuid());

      stream.append({ event: "666" });
      stream.append({ event: "777" });
      await stream.commit(uuid());

      expect(stream.getCommittedEvents()[1].version).toBe(1);
      expect(commitCount).toBe(2);
    });
  });
});

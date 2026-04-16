import { describe, it, expect } from "vitest";
import Promise from "bluebird";

import EventStore from "tapeworm";
import IdbPersistenceStore from "..";

import getDb from "./util";

describe("event_store", () => {
  describe("#openPartition", () => {
    it("should return non null partition when using new id", async () => {
      var es = new EventStore(new IdbPersistenceStore(getDb()));
      var partition = await es.openPartition("location");
      expect(partition).not.toBeNull();
    });
    it("should return same instance when called multiple times", async () => {
      var es = new EventStore(new IdbPersistenceStore(getDb()));
      var [p1, p2] = await Promise.all([
        es.openPartition("location"),
        es.openPartition("location"),
      ]);
      expect(p1).toBe(p2);
    });
    it("should return different instances for different partitionIds", async () => {
      var es = new EventStore(new IdbPersistenceStore(getDb()));
      var [p1, p2] = await Promise.all([
        es.openPartition("location"),
        es.openPartition("location2"),
      ]);
      expect(p1).not.toBe(p2);
    });
  });
});

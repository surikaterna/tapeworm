import Promise from "bluebird";
import { describe, expect, it } from "vitest";
import EventStore from "..";

describe("event_store", () => {
  describe("#openPartition", () => {
    it("should return non null partition when using new id", async () => {
      const es = new EventStore();
      const partition = await es.openPartition("location");
      expect(partition).not.toBeNull();
    });

    it("should return same instance when called multiple times", async () => {
      const es = new EventStore();
      const [p1, p2] = await Promise.all([es.openPartition("location"), es.openPartition("location")]);
      expect(p1).toBe(p2);
    });

    it("should return different instances for different partitionIds", async () => {
      const es = new EventStore();
      const [p1, p2] = await Promise.all([es.openPartition("location"), es.openPartition("location2")]);
      expect(p1).not.toBe(p2);
    });
  });
});

import { describe, it, expect, beforeEach } from "vitest";
import Tapeworm, { Commit } from "tapeworm";
import { EventStreamSynchronizer } from "../lib/client";

describe("EventStreamSynchronizer", function () {
  var part: ReturnType<typeof Tapeworm.prototype.openPartition> extends Promise<
    infer T
  >
    ? T
    : never;
  var synchronizer: InstanceType<typeof EventStreamSynchronizer>;

  beforeEach(async function () {
    var tapeworm = new Tapeworm();
    var blondie = await tapeworm.openPartition("blondie");
    part = blondie;
    synchronizer = new EventStreamSynchronizer(
      part as ConstructorParameters<typeof EventStreamSynchronizer>[0],
    );
  });

  describe("#_processCommits", function () {
    it("should do nothing if commits matches", async function () {
      var commits = [
        new Commit("1", "location", "1", 0, [{ id: "1", type: "alloha" }]),
        new Commit("2", "location", "1", 1, [{ id: "2", type: "alloha" }]),
      ];
      await (part as unknown as Record<string, Function>).append(commits);
      await synchronizer._processCommits(commits, "1");
      var stream = await (
        part as unknown as Record<string, Function>
      ).openStream("1");
      expect(stream.getVersion()).toBe(2);
    });
    it("should do append if seq is ok", async function () {
      var commits = [
        new Commit("1", "location", "1", 0, [{ id: "1", type: "alloha" }]),
        new Commit("2", "location", "1", 1, [{ id: "2", type: "alloha" }]),
      ];
      await (part as unknown as Record<string, Function>).append(commits);
      var commit = new Commit("3", "location", "1", 2, [
        { id: "3", type: "alloha" },
      ]);
      var conflict = await synchronizer._processCommits([commit], "1");
      expect(conflict).toBe(false);
      var stream = await (
        part as unknown as Record<string, Function>
      ).openStream("1");
      expect(stream.getVersion()).toBe(3);
    });
    it("should do trunk and indicate conflict if new commit", async function () {
      var commits = [
        new Commit("1", "location", "1", 0, [
          { id: "1", type: "alloha.registered" },
        ]),
        new Commit("2", "location", "1", 1, [
          { id: "2", type: "alloha.amended" },
        ]),
      ];
      await (part as unknown as Record<string, Function>).append(commits);
      var conflict = await synchronizer._processCommits(
        [
          new Commit("3", "location", "1", 1, [
            { id: "3", type: "alloha.removed" },
          ]),
        ],
        "1",
      );
      expect(conflict).toBe(true);
      var stream = await (
        part as unknown as Record<string, Function>
      ).openStream("1");
      expect(stream.getVersion()).toBe(2);
    });
  });
  describe.skip("#_applyNewCommit", function () {
    it("should add if seq ok", async function () {
      var commits = [
        new Commit("1", "location", "1", 0, [{ id: "1", type: "alloha" }]),
        new Commit("2", "location", "1", 1, [{ id: "2", type: "alloha" }]),
      ];
      await (part as unknown as Record<string, Function>).append(commits);
      await (
        synchronizer as unknown as Record<string, Function>
      )._applyNewCommit(
        new Commit("3", "location", "1", 2, [{ id: "1", type: "alloha" }]),
        1,
      );
    });
    it("should throw if skipping seq", async function () {
      var commits = [
        new Commit("1", "location", "1", 0, [{ id: "1", type: "alloha" }]),
        new Commit("2", "location", "1", 1, [{ id: "2", type: "alloha" }]),
      ];
      await (part as unknown as Record<string, Function>).append(commits);
      expect(function () {
        (synchronizer as unknown as Record<string, Function>)._applyNewCommit(
          new Commit("3", "location", "1", 3, [{ id: "1", type: "alloha" }]),
          1,
        );
      }).toThrow();
    });
  });
});

import { MongoClient } from "mongodb";
import type { Db, MongoClient as MongoClientType } from "mongodb";
import { MongoMemoryReplSet } from "mongodb-memory-server";
import Promise from "bluebird";
import EventStore, {
  Commit,
  Event,
  ConcurrencyError,
  DuplicateCommitError,
} from "tapeworm";
import Store from "..";

function uuid(): string {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

let mongoServer: MongoMemoryReplSet;

describe("indexeddb_persistence", () => {
  var _db: Db | null = null;
  var _client: MongoClientType | null = null;
  const getDb = () => _db!;

  beforeAll(async () => {
    mongoServer = await MongoMemoryReplSet.create({
      replSet: { count: 1, storageEngine: "wiredTiger" },
    });
    const uri = mongoServer.getUri();
    const mongoClient = await MongoClient.connect(uri);
    const db = mongoClient.db("db_test_suite");
    _client = mongoClient;
    _db = db;
  }, 60000);

  beforeEach(async () => {
    try {
      await _db!.collection("tw_1_commits").drop();
      await _db!.collection("tw_1_snapshots").drop();
    } catch (e) {}
  });

  afterAll(async () => {
    if (_client) {
      await _client.close();
    }
    if (mongoServer) {
      await mongoServer.stop();
    }
  });

  describe("#commit", function () {
    test("should accept a commit and store it", async () => {
      var store = new Store(getDb());
      var partition = await store.openPartition("1");
      var events = [new Event(uuid(), "type1", { test: 11 })];
      var commit = new Commit(uuid(), "master", "1", 0, events);
      await partition.append(commit);
      var x = await partition.queryAll();
      expect(x.length).toEqual(1);
    });

    test("commit in one stream is not visible in other", async () => {
      var store = new Store(getDb());
      var partition = await store.openPartition("1");
      var events1 = [new Event(uuid(), "type1", { test: 11 })];
      var commit1 = new Commit(uuid(), "master", "1", 0, events1);
      await partition.append(commit1);

      var events2 = [new Event(uuid(), "type2", { test: 22 })];
      var commit2 = new Commit(uuid(), "master", "2", 0, events2);
      await partition.append(commit2);

      var [r1, r2] = await Promise.all([
        partition.queryStream("1"),
        partition.queryStream("2"),
      ]);
      expect(r1.length).toEqual(1);
      expect(r2.length).toEqual(1);
    });

    test("two commits in one stream are visible", async () => {
      var store = new Store(getDb());
      var partition = await store.openPartition("1");
      var events1 = [new Event(uuid(), "type1", { test: 11 })];
      var commit1 = new Commit(uuid(), "master", "1", 0, events1);
      await partition.append(commit1);

      var events2 = [new Event(uuid(), "type2", { test: 22 })];
      var commit2 = new Commit(uuid(), "master", "1", 1, events2);
      await partition.append(commit2);

      var res = await partition.queryAll();
      expect(res.length).toEqual(2);
    });
  });

  test("query stream from version without fallback", async () => {
    var store = new Store(getDb());
    var partition = await store.openPartition("1");
    var streamId = uuid();
    var events = [
      new Event(uuid(), "event-1", { test: 11, version: 0 }),
      new Event(uuid(), "event-2", { test: 12, version: 1 }),
      new Event(uuid(), "event-3", { test: 13, version: 2 }),
    ];
    events.forEach(function (e) {
      e.version = e.payload.version as number;
    });
    var commit = new Commit(uuid(), "master", streamId, 0, events);
    await partition.append(commit);

    var events2 = [
      new Event(uuid(), "event-4", { test: 14, version: 3 }),
      new Event(uuid(), "event-5", { test: 15, version: 4 }),
      new Event(uuid(), "event-6", { test: 16, version: 5 }),
    ];
    events2.forEach(function (e) {
      e.version = e.payload.version as number;
    });
    var commit2 = new Commit(uuid(), "master", streamId, 1, events2);
    await partition.append(commit2);

    var res = await partition.queryStream(streamId, 4);
    expect(res[0].events[0].version).toEqual(4);
    expect(res[0].events[1].version).toEqual(5);
    expect(res[0].commitSequence).toEqual(1);
  });

  test("query stream from version lead to empty results", async () => {
    var store = new Store(getDb());
    var partition = await store.openPartition("1");
    var streamId = uuid();
    var events = [
      new Event(uuid(), "event-1", { test: 11, version: 0 }),
      new Event(uuid(), "event-2", { test: 12, version: 1 }),
      new Event(uuid(), "event-3", { test: 13, version: 2 }),
    ];
    events.forEach(function (e) {
      e.version = e.payload.version as number;
    });
    var commit = new Commit(uuid(), "master", streamId, 0, events);
    await partition.append(commit);

    var events2 = [
      new Event(uuid(), "event-4", { test: 14, version: 3 }),
      new Event(uuid(), "event-5", { test: 15, version: 4 }),
      new Event(uuid(), "event-6", { test: 16, version: 5 }),
    ];
    events2.forEach(function (e) {
      e.version = e.payload.version as number;
    });
    var commit2 = new Commit(uuid(), "master", streamId, 1, events2);
    await partition.append(commit2);

    var res = await partition.queryStream(streamId, 6);
    expect(res).toEqual([]);
  });

  test("query stream from version with fallback", async () => {
    var store = new Store(getDb());
    var partition = await store.openPartition("1");
    var streamId = uuid();
    var events = [
      new Event(uuid(), "event-1", { test: 11, version: 0 }),
      new Event(uuid(), "event-2", { test: 12, version: 1 }),
      new Event(uuid(), "event-3", { test: 13, version: 2 }),
    ];
    events.forEach(function (e) {
      e.version = e.payload.version as number;
    });
    var commit = new Commit(uuid(), "master", streamId, 0, events);
    await partition.append(commit);

    var events2 = [
      new Event(uuid(), "event-4", { test: 14, version: 3 }),
      new Event(uuid(), "event-5", { test: 15, version: 4 }),
      new Event(uuid(), "event-6", { test: 16, version: 5 }),
    ];
    events2.forEach(function (e) {
      e.version = e.payload.version as number;
    });
    var commit2 = new Commit(uuid(), "master", streamId, 1, events2);
    await partition.append(commit2);

    var res = await partition.queryStream(streamId, 2);
    expect(res.length).toEqual(2);
    expect(res[0].events[0].version).toEqual(2);
    expect(res[1].events[2].version).toEqual(5);
    expect(res[0].commitSequence).toEqual(0);
    expect(res[1].commitSequence).toEqual(1);
  });

  test("#getLatestCommit", async () => {
    var store = new Store(getDb());
    var partition = await store.openPartition("1");
    var streamId = uuid();

    var res = await partition.getLatestCommit(streamId);
    expect(res).toBeUndefined();

    var events = [
      new Event(uuid(), "event-1", { test: 11, version: 0 }),
      new Event(uuid(), "event-2", { test: 12, version: 1 }),
    ];
    events.forEach(function (e) {
      e.version = e.payload.version as number;
    });
    var commit = new Commit(uuid(), "master", streamId, 0, events);
    await partition.append(commit);

    var events2 = [
      new Event(uuid(), "event-4", { test: 14, version: 3 }),
      new Event(uuid(), "event-5", { test: 15, version: 4 }),
      new Event(uuid(), "event-6", { test: 16, version: 5 }),
    ];
    events2.forEach(function (e) {
      e.version = e.payload.version as number;
    });
    var commit2 = new Commit(uuid(), "master", streamId, 1, events2);
    await partition.append(commit2);

    res = await partition.getLatestCommit(streamId);
    expect(res!.commitSequence).toEqual(1);
    expect(res!.events.length).toEqual(3);
    expect(
      (
        res!.events[2] as Record<string, unknown> & {
          payload: Record<string, unknown>;
        }
      ).payload.test,
    ).toEqual(16);
  });

  test("#truncateStreamFrom", async () => {
    var store = new Store(getDb());
    var partition = await store.openPartition("1");
    var firstStreamId = uuid();
    var secondStreamId = uuid();

    await Promise.all([
      partition.append(
        new Commit(uuid(), "master", firstStreamId, 0, [
          new Event(uuid(), "event-1-1", { test: 11, version: 0 }),
        ]),
      ),
      partition.append(
        new Commit(uuid(), "master", secondStreamId, 0, [
          new Event(uuid(), "event-2-1", { test: 21, version: 0 }),
        ]),
      ),
      partition.append(
        new Commit(uuid(), "master", firstStreamId, 1, [
          new Event(uuid(), "event-1-2", { test: 12, version: 1 }),
        ]),
      ),
      partition.append(
        new Commit(uuid(), "master", secondStreamId, 1, [
          new Event(uuid(), "event-2-2", { test: 22, version: 1 }),
        ]),
      ),
    ]);

    await partition.truncateStreamFrom(firstStreamId, 1);
    var r1 = await partition.queryStream(firstStreamId);
    var r2 = await partition.queryStream(secondStreamId);
    expect(r1.length).toEqual(1);
    expect(r1[0].commitSequence).toEqual(0);
    expect(r2.length).toEqual(2);

    await partition.truncateStreamFrom(secondStreamId, -1);
    r1 = await partition.queryStream(firstStreamId);
    r2 = await partition.queryStream(secondStreamId);
    expect(r1.length).toEqual(1);
    expect(r2.length).toEqual(0);
  });

  describe("#snapshot", function () {
    test("should return previously stored snapshot", async () => {
      var store = new Store(getDb());
      var part = await store.openPartition("1");
      var snapshot = await part.storeSnapshot(
        "stream1",
        { iAmSnapshot: true },
        10,
      );
      var newSnapshot = await part.loadSnapshot("stream1");
      expect(newSnapshot).toEqual(snapshot);
    });

    test("should remove snapshot", async () => {
      var store = new Store(getDb());
      var part = await store.openPartition("1");
      var snapshot = await part.storeSnapshot(
        "stream1",
        { iAmSnapshot: true },
        10,
      );
      expect(snapshot.version).toEqual(10);
      var updated = await part.removeSnapshot("stream1");
      expect(updated.version).toEqual(-1);
    });
  });

  describe("#concurrency", function () {
    test("same commit sequence twice should throw", async () => {
      var store = new Store(getDb());
      var partition = await store.openPartition("1");
      var events = [new Event(uuid(), "type1", { test: 11 })];
      var commit = new Commit(uuid(), "master", "1", 0, events);
      var commit2 = new Commit(uuid(), "master", "1", 0, events);

      await expect(
        Promise.all([partition.append(commit), partition.append(commit2)]),
      ).rejects.toThrow(ConcurrencyError);
    });
  });

  describe("#duplicateEvents", function () {
    test("same commit twice should throw", async () => {
      var store = new Store(getDb());
      var partition = await store.openPartition("1");
      var events = [new Event(uuid(), "type1", { test: 11 })];
      var commit = new Commit(uuid(), "master", "1", 0, events);
      await partition.append(commit);

      await expect(partition.append(commit)).rejects.toThrow();
    });
  });

  describe("#partition", function () {
    test("getting the same partition twice should return same instance", async () => {
      var store = new Store(getDb());
      var [p1, p2] = await Promise.all([
        store.openPartition("1"),
        store.openPartition("1"),
      ]);
      expect(p1).toBe(p2);
    });

    test("not indicating partition name should give master partition", async () => {
      var store = new Store(getDb());
      var [p1, p2] = await Promise.all([
        store.openPartition(),
        store.openPartition("master"),
      ]);
      expect(p1).toBe(p2);
    });
  });
});

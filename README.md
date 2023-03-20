Tapeworm
========

[Tape Write Once, Read Many](https://github.com/surikaterna/tapeworm) is a library for event sourcing for in Node.

* [Purpose](#purpose)
* [Installation](#installation)
* [Usage](#usage)
  * [In Memory](#in-memory)
  * [Custom Partition](#custom-partition)
* [Components](#components)
  * [EventStore](#eventstore)
    * [Methods](#methods)
      * [openPartition](#openpartition)
    * [EventStorePartition](#eventstorepartition)
      * [Methods](#methods-1)
        * [openStream](#openstream)
        * [append](#append)
        * [delete](#delete)
      * [Persistence Partition Wrapper Methods](#persistence-partition-wrapper-methods)
        * [queryStreamWithSnapshot](#querystreamwithsnapshot)
        * [storeSnapshot](#storesnapshot)
        * [loadSnapshot](#loadsnapshot)
        * [queryStream](#querystream)
        * [removeSnapshot](#removesnapshot)
        * [getLatestCommit](#getlatestcommit)
        * [querySnapshotsByMaxDateTime](#querysnapshotsbymaxdatetime)
  * [EventStream](#eventstream)
    * [Methods](#methods-2)
      * [getVersion](#getversion)
      * [append](#append-1)
      * [hasChanges](#haschanges)
      * [commit](#commit)
      * [revertChanges](#revertchanges)
      * [getCommittedEvents](#getcommittedevents)
      * [getUncommittedEvents](#getuncommittedevents)
  * [Commit](#commit-1)
  * [Event](#event)

# Purpose

Tapeworm is an event store, configurable with an external persistence partition. By default it provides its own in
memory persistence partition, but it can be exchanged by e.g.
a [MongoDB persistence partition](https://github.com/surikaterna/tapeworm_persistence_store_mongodb) or
an [IndexedDB persistence partition](https://github.com/surikaterna/tapeworm_persistence_store_indexdb).

# Installation

```shell
npm install tapeworm
```

# Usage

## In Memory

```js
import EventStore from 'tapeworm';

// A singleton Tapeworm EventStore should be instantiated somewhere
const eventStore = new EventStore();

// Opening the same partition multiple times returns the same Partition instance
const partition = await eventStore.openPartition('location');

const firstCommit = new Commit('1', 'location', '1', 0, []);
const secondCommit = new Commit('2', 'location', '1', 1, []);
const thirdCommit = new Commit('3', 'location', '1', 2, []);

// When all commits have ben persisted, the commits are returned
const commits = await partition.append([firstCommit, secondCommit]);
// [firstCommit, secondCommit]

// Single commits are returned as an array of one item
const addedCommits = await partition.append(thirdCommit);
// [thirdCommit]
```

## Custom Partition

This example uses
the [MongoDB persistence partition](https://github.com/surikaterna/tapeworm_persistence_store_mongodb), but a custom
persistence partition can be implemented and used as needed.

```js
const dispatchService = (commit) => {
  // Commit dispatched from the custom store, handle it as necessary
}

const eventStore = new EventStore(tapewormMdbStore, dispatchService);
const partition = await eventStore.openPartition('location');

const commits = [
  new Commit('1', 'location', '1', 0, []),
  new Commit('2', 'location', '1', 1, [])
];

// Will call dispatchService twice, once for each commit when handled by the custom partition
await partition.append(commits);
```

# Components

## EventStore

The TapeWORM EventStore is exported as default. It takes an optional custom partition and an optional dispatch service
and holds a collection of [event store partitions](#eventstorepartition) opened.

```js
import EventStore from 'tapeworm';

// A singleton TapeWORM EventStore should be instantiated somewhere
const eventStore = new EventStore();
```

### Methods

#### openPartition

Resolves an [EventStorePartition](#eventstorepartition) when the persistence store has successfully opened its
persistence partition.

```js
const partition = await eventStore.openPartition();
```

### EventStorePartition

A wrapper around the persistence partition.

### Methods

#### openStream

Resolves an [EventStream](#eventstream) when it has properly loaded all commits.

```js
const eventStream = await partition.openStream(streamId, writeOnly, callback);
```

#### append

Takes a commit or a list of commits and appends them on the persistence partition. Will call the dispatch service, if
provided, once per appended commit.

```js
const appendedCommits = await partition.append(commits, callback);
```

#### delete

Commit a stream deleted event to indicate that all related data for the stream shall be removed.

```js
await partition.delete(streamId, deleteEvent);
```

### Persistence Partition Wrapper Methods

The EventStorePartition will provide methods for calling the following methods directly on the persistence partition,
providing the provided arguments.

#### queryStreamWithSnapshot

Has a fallback implementation assuming that the persistence partition used has the [loadSnapshot](#loadsnapshot)
and [queryStream](#querystream) methods implemented.

#### storeSnapshot

Store a snapshot for a stream and resolve data for the stored snapshot.

```js
const snapshotData = await partition.storeSnapshot(streamId, snapshot, version, callback);
// Result: { id, version, snapshot }
```

#### loadSnapshot

Retrieve the stored snapshot of a stream.

```js
const snapshotData = await partition.loadSnapshot(streamId, callback);
// Result: { id, version, snapshot }
```

#### queryStream

Retrieve the commits for a stream.

```js
const snapshotData = await partition.queryStream(streamId, fromEventSequence, callback);
// Result: [{ id, streamId, commitSequence, events }, { id, streamId, commitSequence, events }]
```

#### removeSnapshot

Remove the stored snapshot of a stream.

```js
await partition.removeSnapshot(streamId, callback);
// Result: { id, version, snapshot }
```

#### getLatestCommit

Retrieve the last stored commit for a stream.

```js
const commit = await partition.getLatestCommit(streamId, callback);
// Result: { id, streamId, commitSequence, events }
```

#### querySnapshotsByMaxDateTime

Retrieve the stored snapshots older than the provided date string.

```js
const snapshots = await partition.querySnapshotsByMaxDateTime(dateTime, callback);
// Result: [{ id }, { id }]
```

## EventStream

### Methods

#### getVersion

Retrieves the current version of the stream.

```js
const version = eventStream.getVersion();
```

#### append

Add an [Event](#event) to the streams list of uncommitted (planned) events.

```js
eventStream.append(event);
```

#### hasChanges

Returns whether there are uncommitted events on the stream.

```js
const hasChanges = eventStream.hasChanges();
```

#### commit

Build a [commit](#commit) from the uncommitted [events](#event) and append it to
the [event store partition](#eventstorepartition).

```js
await eventStream.commit(commitId, callback);
```

#### revertChanges

Remove the uncommitted events from the stream to prevent the changes from being committed.

```js
eventStream.revertChanges();
```

#### getCommittedEvents

Returns a copy of the committed events for the stream.

Throws an error if the stream is created as _write only_.

```js
const events = eventStream.getCommittedEvents();
```

#### getUncommittedEvents

Returns a copy of the uncommitted events appended to the stream.

```js
const events = eventStream.getUncommittedEvents();
```

## Commit

Creates a new Commit instance.

```js
const commit = new Commit(id, partitionId, streamId, commitSequence, events);

// commit.id: id
// commit.partitionId: partitionId
// commit.streamId: streamId
// commit.commitSequence: commitSequence
// commit.events: events
```

## Event

Creates a new Event instance.

```js
const event = new Event(id, type, data, metadata);

// event.id: id
// event.type: type
// event.data: data
// event.metadata: metadata (defaults to {})
// event.timestamps: Date (date of creation)
// event.revision: null (to be updated by the event store when appended)
```

## Dispatch Service

The Tapeworm event store takes a dispatch service as an optional second argument. If provided, it will be called with
the commit as payload once it has been processed by the persistence partition.

```js
function dispatchService(commit, markAsDispatched) {
  // Distribute information about the processed commit
  // ...

  markAsDispatched();
}

const eventStore = new EventStore(null, dispatchService);
const partition = await eventStore.openPartition('location');

// Will call dispatchService when the commit is processed
partition.append(new Commit('4', 'location', '1', 3, []));
```

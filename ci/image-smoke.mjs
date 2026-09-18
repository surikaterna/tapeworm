// @ts-check
import assert from 'node:assert/strict';
import { readFileSync, existsSync } from 'node:fs';
import { setTimeout as delay } from 'node:timers/promises';
import { MongoClient, UUID } from 'mongodb';
import { connect } from 'amqplib';
import { MongoResumeTokenStore, checkpointFeed, Dispatcher } from 'tapeworm_dispatcher_mdb_rmq';
import * as core from 'tapeworm';
import { runtimeBuilds, sameRuntimeBuilds } from './artifacts.mjs';

const uri = 'mongodb://mongo:27017/?directConnection=true&replicaSet=rs0';
const rabbitUri = 'amqp://ci:ci-test-only@rabbit:5672';
const client = new MongoClient(uri, { serverSelectionTimeoutMS: 5000, socketTimeoutMS: 15000 });
const db = client.db('ci_smoke');
const store = new MongoResumeTokenStore(db, 'checkpoint', { checkpointKey: 'ci-smoke' });
const config = { mongodb: { db, collection: 'commits' }, rabbitmq: { uri: rabbitUri, exchange: 'ci_smoke' }, feedId: 'ci-smoke' };
/** @param {number} n */
const token = n => `00000000-0000-7000-8000-${String(n).padStart(12, '0')}`;
/** @param {number} n */
const commit = n => ({ id: `commit-${n}`, partitionId: 'master', streamId: 'stream', commitSequence: n,
  token: new UUID(token(n)), events: [{ id: `event-${n}`, type: 'test', payload: { n } }] });

/** @param {unknown} value @returns {Record<string, unknown>} */
function record(value) {
  assert.ok(value && typeof value === 'object' && !Array.isArray(value));
  return Object.fromEntries(Object.entries(value));
}

/** @param {string} file */
const json = file => record(JSON.parse(readFileSync(file, 'utf8')));

/** @param {() => Promise<boolean>} check */
async function eventually(check) {
  const deadline = Date.now() + 15000;
  while (!(await check())) {
    assert.ok(Date.now() < deadline, 'image probe condition timed out');
    await delay(50);
  }
}

async function services() {
  for (const host of ['mongo', 'expiry']) {
    const mongo = new MongoClient(`mongodb://${host}:27017/?directConnection=true&replicaSet=rs0`, { serverSelectionTimeoutMS: 5000 });
    try {
      const admin = mongo.db().admin();
      assert.equal((await admin.command({ buildInfo: 1 })).version, '8.3.9');
      assert.equal((await admin.command({ hello: 1 })).isWritablePrimary, true);
      assert.equal((await admin.command({ replSetGetStatus: 1 })).set, 'rs0');
      if (host === 'expiry') {
        assert.equal((await mongo.db('local').command({ collStats: 'oplog.rs' })).maxSize, 1048576);
        assert.equal((await admin.command({ getParameter: 1, syncdelay: 1 })).syncdelay, 1);
      }
      console.log('Verified Mongo', host, '8.3.9 primary rs0', host === 'expiry' ? '1MiB syncdelay=1' : '');
    } finally { await mongo.close(); }
  }
  const rabbit = await connect(rabbitUri, { timeout: 5000 });
  await rabbit.close();
  console.log('Verified Rabbit AMQP connection');
}

function identity() {
  assert.equal(process.version, 'v26.9.0');
  assert.notEqual(process.getuid?.(), 0);
  assert.equal(typeof Dispatcher, 'function');
  assert.ok(Object.keys(core).length > 0);
  assert.equal(existsSync('/app/packages/tapeworm_dispatcher_mdb_rmq/dist/ci-host-sentinel'), false);
  for (const dependency of ['typescript', 'vitest', 'turbo', 'eslint']) {
    assert.equal(existsSync(`/app/node_modules/${dependency}`), false, `${dependency} leaked into runtime`);
  }
  const expected = json('/evidence/identity.json');
  const actualBuilds = runtimeBuilds('/app');
  sameRuntimeBuilds(expected.runtimeBuilds, actualBuilds);
  console.log('IMAGE runtime inventory/bytes match qualified npm outputs',
    Object.fromEntries(Object.entries(actualBuilds).map(([name, files]) => [name, files.length])));
  assert.equal(json('/app/package.json').version, expected.rootVersion);
  for (const [name, version] of Object.entries(record(expected.versions))) {
    assert.ok(name === 'tapeworm' || name === 'tapeworm_dispatcher_mdb_rmq');
    const actual = json(`/app/packages/${name}/package.json`);
    assert.equal(actual.version, version);
  }
  console.log('IMAGE IDENTITY', { node: process.version, uid: process.getuid?.(), versions: expected.versions });
}

async function prepare() {
  await db.dropDatabase();
  await db.createCollection('commits');
  await db.collection('commits').createIndex({ token: 1 });
  const session = client.startSession({ causalConsistency: true });
  try {
    await db.collection('commits').findOne({}, { session, readConcern: { level: 'majority' } });
    assert.ok(session.operationTime);
    await store.save({ version: 1, feed: checkpointFeed(config), updatedAt: new Date(),
      primary: { kind: 'boundary', mode: 'changeStream', ts: session.operationTime } });
  } finally { await session.endSession(); }
  const connection = await connect(rabbitUri, { timeout: 5000 });
  try {
    const channel = await connection.createChannel();
    await channel.assertExchange('ci_smoke', 'headers', { durable: true });
    await channel.assertQueue('ci_smoke', { durable: true });
    await channel.purgeQueue('ci_smoke');
    await channel.bindQueue('ci_smoke', 'ci_smoke', '', { 'x-match': 'all', collection: 'commits' });
  } finally { await connection.close(); }
  await db.collection('commits').insertOne(commit(11), { writeConcern: { w: 'majority' } });
}

/** @param {number} required */
async function messages(required) {
  const connection = await connect(rabbitUri, { timeout: 5000 });
  /** @type {string[]} */
  const ids = [];
  try {
    const channel = await connection.createChannel();
    await eventually(async () => {
      const message = await channel.get('ci_smoke', { noAck: true });
      if (!message) return ids.includes(`commit-${required}`);
      const body = record(JSON.parse(message.content.toString()));
      assert.equal(message.properties.messageId, body.id);
      assert.ok(body.id === 'commit-11' || body.id === 'commit-12');
      assert.ok(typeof body.commitSequence === 'number');
      assert.deepEqual(body.events, commit(body.commitSequence).events);
      ids.push(body.id);
      return false;
    });
  } finally { await connection.close(); }
  console.log('IMAGE RABBIT original bodies and stable IDs', ids);
}

async function operations() {
  const current = await db.admin().command({ currentOp: 1, ns: 'ci_smoke.checkpoint', op: 'update' });
  /** @type {unknown} */
  const values = current.inprog;
  assert.ok(Array.isArray(values), 'Missing currentOp results');
  return values.map((/** @type {unknown} */ value) => {
    assert.ok(value && typeof value === 'object');
    return { waitingForLock: 'waitingForLock' in value && value.waitingForLock === true };
  });
}

async function blocked() {
  await eventually(async () => (await operations()).some(op => op.waitingForLock === true));
  await messages(11);
  assert.equal((await store.load())?.lastCommitToken, undefined);
  console.log('IMAGE checkpoint currentOp waitingForLock=true; Rabbit received before signal');
}

async function locked() {
  assert.equal((await db.admin().command({ currentOp: 1 })).fsyncLock, true);
  assert.equal((await store.load())?.lastCommitToken, undefined);
  console.log('IMAGE exited while fsyncLock=true and checkpoint unchanged (no cancellation claim)');
}

async function next() {
  await eventually(async () => (await operations()).length === 0);
  await db.collection('shutdown_barrier').insertOne({}, { writeConcern: { w: 'majority' } });
  assert.ok([undefined, token(11)].includes((await store.load())?.lastCommitToken));
  await db.collection('commits').insertOne(commit(12), { writeConcern: { w: 'majority' } });
}

/** @param {number} n */
async function delivered(n) {
  await eventually(async () => (await store.load())?.lastCommitToken === token(n));
  await messages(n);
  console.log('IMAGE majority checkpoint', token(n));
}

try {
  switch (process.argv[2]) {
    case 'identity': identity(); break;
    case 'services': await services(); break;
    case 'prepare': case 'prepare-lock': await prepare(); break;
    case 'delivered': await delivered(11); break;
    case 'lock': await db.admin().command({ fsync: 1, lock: true }); break;
    case 'blocked': await blocked(); break;
    case 'locked': await locked(); break;
    case 'next': await next(); break;
    case 'progressed': await delivered(12); break;
    default: throw new Error('Unknown image probe');
  }
} finally { await client.close(); }

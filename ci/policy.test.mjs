// @ts-check
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { registry, releasePolicy, sameIdentity } from './policy.mjs';

test('release permits only already-versioned clean master', () => {
  releasePolicy('master', '', [], '');
});
test('non-master branch, pull request, and tag contexts cannot publish', () => {
  const contexts = ['develop', 'release/1.2.0', 'main', 'feature/test', 'PR-43', 'v1.2.0', ''];
  for (const branch of contexts) {
    assert.throws(() => releasePolicy(branch, '', [], ''), /master-only/);
  }
});
test('tag metadata rejects publication even when the branch context says master', () => {
  for (const tag of ['master', 'v1.2.0']) {
    assert.throws(() => releasePolicy('master', tag, [], ''), /Tag contexts/);
  }
});
test('pending changesets reject release', () => {
  assert.throws(() => releasePolicy('master', '', ['pending.md'], ''), /pending changesets/);
});
test('tracked or untracked source dirt rejects release', () => {
  for (const status of [' M package.json', '?? secret']) assert.throws(() => releasePolicy('master', '', [], status), /clean/);
});
test('registry host parsing preserves namespace and port', () => {
  assert.deepEqual(registry('ghcr.io/org'), { host: 'ghcr.io', prefix: 'ghcr.io/org' });
  assert.deepEqual(registry('registry.example:5000/team'), { host: 'registry.example:5000', prefix: 'registry.example:5000/team' });
});
test('registry rejects schemes, shell syntax and credential URLs', () => {
  for (const value of ['https://ghcr.io/org', 'user:password@host', 'host;false', '-bad', 'ghcr.io/org/']) {
    assert.throws(() => registry(value));
  }
});
const identity = { revision: 'abc', rootVersion: '0.0.0', versions: { tapeworm: '1.0.0' }, builds: '123', runtimeBuilds: {} };
test('exact artifact identity passes', () => sameIdentity(identity, structuredClone(identity)));
test('artifact version, revision and build mismatches reject', () => {
  for (const changed of [{ revision: 'def' }, { versions: { tapeworm: '2.0.0' } }, { builds: '456' }, { rootVersion: '1.0.0' }]) {
    assert.throws(() => sameIdentity(identity, { ...identity, ...changed }), /identity changed/);
  }
});

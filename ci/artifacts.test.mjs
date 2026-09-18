// @ts-check
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { cleanOutputs, inventory, runtimeBuilds, sameRuntimeBuilds, runtimePackages, verifyNpmRuntimeFiles } from './artifacts.mjs';
import { identity, sameIdentity } from './policy.mjs';

/** @param {(root: string, external: string) => void} check */
function fixture(check) {
  const scratch = mkdtempSync(join(tmpdir(), 'artifact safety space '));
  const root = join(scratch, 'repo'); const external = join(scratch, 'external');
  mkdirSync(root); mkdirSync(external); writeFileSync(join(external, 'foreign'), 'preserve');
  try {
    writeFileSync(join(root, 'package.json'), JSON.stringify({ private: true, version: '0.0.0', workspaces: ['packages/*'] }));
    writeFileSync(join(root, 'tsconfig.base.json'), JSON.stringify({ compilerOptions: {} }));
    for (const name of runtimePackages) {
      const directory = join(root, 'packages', name); mkdirSync(directory, { recursive: true });
      writeFileSync(join(directory, 'package.json'), JSON.stringify({ name, version: '1.0.0', files: ['dist'] }));
      writeFileSync(join(directory, 'tsconfig.json'), JSON.stringify({ extends: '../../tsconfig.base.json', compilerOptions: { outDir: 'dist' } }));
    }
    const worker = join(root, 'packages', 'tapeworm_dispatcher_mdb_rmq', 'tsconfig.worker.json');
    writeFileSync(worker, JSON.stringify({ extends: './tsconfig.json', compilerOptions: { outDir: 'dist-worker' } }));
    execFileSync('git', ['init', '-q'], { cwd: root });
    check(root, external);
    assert.equal(readFileSync(join(external, 'foreign'), 'utf8'), 'preserve');
  } finally { rmSync(scratch, { recursive: true, force: true }); }
}

/** @param {string} root */
function seedBuilds(root) {
  for (const name of runtimePackages) {
    const directory = join(root, 'packages', name, 'dist'); mkdirSync(directory);
    writeFileSync(join(directory, 'index.js'), 'exports.version = 1;\n');
  }
  return join(root, 'packages', 'tapeworm_dispatcher_mdb_rmq', 'dist');
}

test('pristine cleanup bootstraps nonexistent outputs in a space-containing workspace', () => fixture(root => {
  cleanOutputs(root); cleanOutputs(root);
  assert.ok(existsSync(join(root, 'packages', 'tapeworm', 'package.json')));
}));

test('cleanup removes stale declared dist/worker outputs but preserves other ignored data', () => fixture(root => {
  const output = seedBuilds(root);
  writeFileSync(join(output, 'obsolete.js'), 'stale');
  const worker = join(root, 'packages', 'tapeworm_dispatcher_mdb_rmq', 'dist-worker'); mkdirSync(worker);
  writeFileSync(join(worker, 'old.js'), 'stale');
  const cache = join(root, 'node_modules'); mkdirSync(cache); writeFileSync(join(cache, 'keep'), 'keep');
  writeFileSync(join(root, '.env.local'), 'synthetic keep');
  cleanOutputs(root);
  assert.equal(existsSync(output), false); assert.equal(existsSync(worker), false);
  assert.equal(readFileSync(join(cache, 'keep'), 'utf8'), 'keep');
  assert.equal(readFileSync(join(root, '.env.local'), 'utf8'), 'synthetic keep');
}));

test('output symlink is refused before any workspace output is deleted', () => fixture((root, external) => {
  seedBuilds(root);
  const output = join(root, 'packages', 'tapeworm_dispatcher_mdb_rmq', 'dist'); rmSync(output, { recursive: true });
  symlinkSync(external, output);
  assert.throws(() => cleanOutputs(root), /symlink output/);
  assert.ok(existsSync(join(root, 'packages', 'tapeworm', 'dist', 'index.js')));
}));

test('symlinked workspace and root are rejected without following external directories', () => fixture((root, external) => {
  symlinkSync(external, join(root, 'packages', 'foreign'));
  assert.throws(() => cleanOutputs(root), /owned directory/);
  const link = join(external, 'repo-link'); symlinkSync(root, link);
  assert.throws(() => cleanOutputs(link), /owned directory/);
}));

test('nested output symlink is only unlinked, never traversed', () => fixture((root, external) => {
  const output = seedBuilds(root); symlinkSync(external, join(output, 'foreign-link'));
  cleanOutputs(root); assert.equal(existsSync(output), false);
}));

test('tracked output files stop cleanup before any deletion', () => fixture(root => {
  const output = seedBuilds(root); writeFileSync(join(output, 'tracked.js'), 'tracked source');
  execFileSync('git', ['add', '-f', 'packages/tapeworm_dispatcher_mdb_rmq/dist/tracked.js'], { cwd: root });
  assert.throws(() => cleanOutputs(root), /tracked files/);
  assert.ok(existsSync(join(output, 'tracked.js')));
  assert.ok(existsSync(join(root, 'packages', 'tapeworm', 'dist', 'index.js')));
}));

test('undeclared output or incremental configuration fails closed', () => fixture(root => {
  const config = join(root, 'packages', 'tapeworm', 'tsconfig.json');
  for (const compilerOptions of [{ outDir: '../foreign' }, { outDir: 'dist', incremental: true }]) {
    writeFileSync(config, JSON.stringify({ extends: '../../tsconfig.base.json', compilerOptions }));
    assert.throws(() => cleanOutputs(root));
  }
}));

test('artifact inventory rejects symlinks instead of hashing external data', () => fixture((root, external) => {
  const output = seedBuilds(root); symlinkSync(join(external, 'foreign'), join(output, 'foreign.js'));
  assert.throws(() => inventory(output), /Non-regular artifact/);
}));

test('npm dry-run runtime paths and sizes match the exact built inventory', () => fixture(root => {
  seedBuilds(root); verifyNpmRuntimeFiles(root);
  const manifest = join(root, 'packages', 'tapeworm', 'package.json');
  writeFileSync(manifest, JSON.stringify({ name: 'tapeworm', version: '1.0.0', files: ['README.md'] }));
  assert.throws(() => verifyNpmRuntimeFiles(root), /npm runtime inventory mismatch/);
}));

test('extra, missing and same-size changed runtime files reject image and release identity', () => fixture(root => {
  const output = seedBuilds(root);
  const expected = identity(root, 'source-revision');
  sameIdentity(expected, identity(root, 'source-revision'));
  sameRuntimeBuilds(expected.runtimeBuilds, runtimeBuilds(root));
  const index = join(output, 'index.js'); const original = readFileSync(index);
  for (const name of ['obsolete.js', 'ci-host-sentinel']) {
    const extra = join(output, name); writeFileSync(extra, 'unexpected');
    assert.throws(() => sameIdentity(expected, identity(root, 'source-revision')), /identity changed/);
    assert.throws(() => sameRuntimeBuilds(expected.runtimeBuilds, runtimeBuilds(root)), /inventory\/bytes changed/);
    rmSync(extra);
  }
  writeFileSync(index, 'exports.version = 2;\n');
  assert.throws(() => sameIdentity(expected, identity(root, 'source-revision')), /identity changed/);
  assert.throws(() => sameRuntimeBuilds(expected.runtimeBuilds, runtimeBuilds(root)), /inventory\/bytes changed/);
  rmSync(index);
  assert.throws(() => sameRuntimeBuilds(expected.runtimeBuilds, runtimeBuilds(root)), /inventory\/bytes changed/);
  writeFileSync(index, original); sameIdentity(expected, identity(root, 'source-revision'));
}));

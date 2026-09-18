// @ts-check
// Sequential regression harness only: never called by qualification/publication.
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { existsSync, lstatSync, mkdirSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { identity, sameIdentity } from './policy.mjs';
import { declaredOutputs, json, runtimeBuilds, sameRuntimeBuilds, verifyNpmRuntimeFiles } from './artifacts.mjs';

const root = process.cwd();
const output = join(root, 'packages', 'tapeworm_dispatcher_mdb_rmq', 'dist');
const obsolete = join(output, 'b3qo-obsolete-deleted-module.js');

function seed() {
  assert.ok(declaredOutputs(root).includes(output));
  assert.equal(execFileSync('git', ['status', '--porcelain'], { encoding: 'utf8' }), '');
  const stat = lstatSync(output, { throwIfNoEntry: false });
  assert.ok(!stat || stat.isDirectory(), 'Refuse a symlinked seed output');
  mkdirSync(output, { recursive: true });
  assert.equal(realpathSync(output), output);
  assert.equal(existsSync(obsolete), false);
  writeFileSync(obsolete, 'exports.obsolete = "b3qo regression";\n', { flag: 'wx' });
  assert.equal(execFileSync('git', ['status', '--porcelain'], { encoding: 'utf8' }), '');
  console.log('SEEDED ignored obsolete module before real qualification', obsolete);
}

function qualify() {
  seed();
  const run = `b3qo-${randomUUID()}`;
  try {
    execFileSync('./ci/qualify.sh', { stdio: 'inherit', env: { ...process.env, RUN_ID: run } });
    verify(join(root, '.ci-artifacts', run));
  } finally { rmSync(obsolete, { force: true }); }
}

/** @param {string} art */
function verify(art) {
  assert.equal(existsSync(obsolete), false, 'Obsolete module survived qualification');
  const expected = json(join(art, 'identity.json'));
  const revision = execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim();
  sameIdentity(expected, identity(root, revision));
  verifyNpmRuntimeFiles(root);
  const image = readFileSync(join(art, 'qualified-image-id'), 'utf8').trim();
  assert.match(image, /^sha256:[a-f0-9]{64}$/);
  execFileSync('docker', ['run', '--rm', '--network', 'none', '--entrypoint', 'node',
    '--mount', `type=bind,src=${root}/ci,dst=/app/ci,readonly`, '--mount', `type=bind,src=${art},dst=/evidence,readonly`,
    image, '/app/ci/image-smoke.mjs', 'identity'], { stdio: 'inherit' });
  tamper(expected, revision);
  console.log('SEEDED QUALIFICATION PASS: obsolete absent from npm/image; exact inventories; post-qualification tampering rejected', art);
}

/** @param {unknown} expected @param {string} revision */
function tamper(expected, revision) {
  const index = join(output, 'index.js'); const original = readFileSync(index);
  const before = runtimeBuilds(root);
  const sentinel = join(output, 'ci-host-sentinel');
  try {
    writeFileSync(sentinel, 'post-qualification addition', { flag: 'wx' });
    assert.throws(() => sameIdentity(expected, identity(root, revision)), /identity changed/);
    assert.throws(() => sameRuntimeBuilds(before, runtimeBuilds(root)), /inventory\/bytes changed/);
    rmSync(sentinel);
    writeFileSync(index, Buffer.concat([original, Buffer.from('\n// post-qualification tamper\n')]));
    assert.throws(() => sameIdentity(expected, identity(root, revision)), /identity changed/);
    assert.throws(() => sameRuntimeBuilds(before, runtimeBuilds(root)), /inventory\/bytes changed/);
  } finally { rmSync(sentinel, { force: true }); writeFileSync(index, original); }
  sameIdentity(expected, identity(root, revision));
}

switch (process.argv[2]) {
  case 'seed': seed(); break;
  case 'qualify': qualify(); break;
  case 'verify': assert.ok(process.argv[3]); verify(process.argv[3]); break;
  default: throw new Error('Use seed, qualify, or verify <artifact-directory>');
}

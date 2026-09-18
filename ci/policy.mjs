// @ts-check
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync, readdirSync } from 'node:fs';
import { resolve, join } from 'node:path';
import { fileURLToPath } from 'node:url';

/** @param {string} prefix */
export function registry(prefix) {
  assert.match(prefix, /^[a-zA-Z0-9.-]+(?::[0-9]+)?(?:\/[a-z0-9._-]+)*$/);
  const host = prefix.split('/')[0];
  assert.ok(host && (host.includes('.') || host.includes(':') || host === 'localhost'), 'Explicit registry host required');
  return { host, prefix };
}

/** @param {string} branch @param {string[]} pending @param {string} status */
export function releasePolicy(branch, pending, status) {
  assert.equal(branch, 'main', 'Publication is main-only');
  assert.deepEqual(pending, [], 'Prepare and commit reviewed versions before release; pending changesets');
  assert.equal(status, '', 'Release source must be clean, including untracked files');
}

/** @typedef {{revision: string, versions: Record<string,string>, rootVersion: string, builds: string}} Identity */
/** @param {Identity} expected @param {Identity} actual */
export function sameIdentity(expected, actual) {
  assert.deepEqual(actual, expected, 'Qualified source/build/version identity changed');
}

/** @param {string} file */
function manifestVersion(file) {
  /** @type {unknown} */
  const parsed = JSON.parse(readFileSync(file, 'utf8'));
  assert.ok(parsed && typeof parsed === 'object' && 'version' in parsed && typeof parsed.version === 'string');
  return parsed.version;
}

/** @param {string} dir @returns {string[]} */
function files(dir) {
  return readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))
    .flatMap(entry => entry.isDirectory() ? files(join(dir, entry.name)) : [join(dir, entry.name)]);
}

/** @param {string} root @returns {Identity} */
function identity(root) {
  const hash = createHash('sha256');
  for (const pkg of readdirSync(join(root, 'packages')).sort()) {
    for (const file of files(join(root, 'packages', pkg, 'dist'))) {
      if (file.endsWith('/ci-host-sentinel')) continue;
      hash.update(file.slice(root.length)); hash.update(readFileSync(file));
    }
  }
  const versions = Object.fromEntries(['tapeworm', 'tapeworm_dispatcher_mdb_rmq'].map(name =>
    [name, manifestVersion(join(root, 'packages', name, 'package.json'))]));
  return { revision: execFileSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' }).trim(),
    versions, rootVersion: manifestVersion(join(root, 'package.json')), builds: hash.digest('hex') };
}

/** @param {string} mode @param {string} art */
function main(mode, art) {
  const root = process.cwd();
  if (mode === 'identity') {
    writeFileSync(join(art, 'identity.json'), JSON.stringify(identity(root)) + '\n');
    return;
  }
  assert.equal(mode, 'release-preflight');
  releasePolicy(process.env.BRANCH_NAME ?? '', readdirSync('.changeset').filter(name => name.endsWith('.md') && name !== 'README.md'),
    execFileSync('git', ['status', '--porcelain', '--untracked-files=all'], { encoding: 'utf8' }).trim());
  sameIdentity(JSON.parse(readFileSync(join(art, 'identity.json'), 'utf8')), identity(root));
  console.log('Release preflight PASS: committed versions, clean main source, unchanged qualified builds');
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  assert.ok(process.argv[2] && process.argv[3], 'mode and artifact directory required');
  main(process.argv[2], process.argv[3]);
}

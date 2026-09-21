// @ts-check
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync, writeFileSync, readdirSync } from 'node:fs';
import { resolve, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { allBuildHash, runtimeBuilds, verifyNpmRuntimeFiles } from './artifacts.mjs';

/** @param {string} prefix */
export function registry(prefix) {
  assert.match(prefix, /^[a-zA-Z0-9.-]+(?::[0-9]+)?(?:\/[a-z0-9._-]+)*$/);
  const host = prefix.split('/')[0];
  assert.ok(host && (host.includes('.') || host.includes(':') || host === 'localhost'), 'Explicit registry host required');
  return { host, prefix };
}

/** @param {string} branch @param {string} tag @param {string[]} pending @param {string} status */
export function releasePolicy(branch, tag, pending, status) {
  assert.equal(branch, 'master', 'Publication is master-only');
  assert.equal(tag, '', 'Tag contexts cannot publish');
  assert.deepEqual(pending, [], 'Prepare and commit reviewed versions before release; pending changesets');
  assert.equal(status, '', 'Release source must be clean, including untracked files');
}

/** @typedef {{revision: string, versions: Record<string,string>, rootVersion: string, builds: string,
 * runtimeBuilds: ReturnType<typeof runtimeBuilds>}} Identity */
/** @param {unknown} expected @param {Identity} actual */
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

/** @param {string} root @param {string} revision @returns {Identity} */
export function identity(root, revision) {
  const versions = Object.fromEntries(['tapeworm', 'tapeworm_dispatcher_mdb_rmq'].map(name =>
    [name, manifestVersion(join(root, 'packages', name, 'package.json'))]));
  return { revision, versions, rootVersion: manifestVersion(join(root, 'package.json')),
    builds: allBuildHash(root), runtimeBuilds: runtimeBuilds(root) };
}

/** @param {string} mode @param {string} art */
function main(mode, art) {
  const root = process.cwd();
  const revision = execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim();
  if (mode === 'identity') {
    verifyNpmRuntimeFiles(root);
    writeFileSync(join(art, 'identity.json'), JSON.stringify(identity(root, revision)) + '\n');
    return;
  }
  if (mode === 'verify-artifacts') {
    sameIdentity(JSON.parse(readFileSync(join(art, 'identity.json'), 'utf8')), identity(root, revision));
    verifyNpmRuntimeFiles(root);
    console.log('Qualified artifact inventories and bytes unchanged');
    return;
  }
  assert.equal(mode, 'release-preflight');
  releasePolicy(process.env.BRANCH_NAME ?? '', process.env.TAG_NAME ?? '',
    readdirSync('.changeset').filter(name => name.endsWith('.md') && name !== 'README.md'),
    execFileSync('git', ['status', '--porcelain', '--untracked-files=all'], { encoding: 'utf8' }).trim());
  sameIdentity(JSON.parse(readFileSync(join(art, 'identity.json'), 'utf8')), identity(root, revision));
  verifyNpmRuntimeFiles(root);
  console.log('Release preflight PASS: committed versions, clean master source, unchanged qualified builds');
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  assert.ok(process.argv[2] && process.argv[3], 'mode and artifact directory required');
  main(process.argv[2], process.argv[3]);
}

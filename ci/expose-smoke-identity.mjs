// @ts-check
import assert from 'node:assert/strict';
import { closeSync, constants, fchmodSync, fstatSync, openSync } from 'node:fs';
import { isAbsolute, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

/** @param {string} root @param {string} runId @param {() => void} [afterOpen] */
export function exposeSmokeIdentity(root, runId, afterOpen = () => {}) {
  assert.ok(isAbsolute(root) && resolve(root) === root, 'Expected an absolute checkout root');
  assert.match(runId, /^[a-zA-Z0-9][a-zA-Z0-9-]{0,70}$/, 'Invalid RUN_ID');
  const uid = process.getuid?.();
  assert.notEqual(uid, undefined, 'Linux UID required');
  const dirs = constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW;
  const files = constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK;
  /** @type {number[]} */
  const opened = [];
  /** @param {string} path @param {number} flags @param {'directory' | 'file'} type */
  function ownedOpen(path, flags, type) {
    const fd = openSync(path, flags);
    opened.push(fd);
    const stat = fstatSync(fd);
    assert.equal(stat.uid, uid, `Image-smoke ${type} must be owned by the runner`);
    assert.ok(type === 'directory' ? stat.isDirectory() : stat.isFile(), `Image-smoke ${type} has wrong type`);
    return fd;
  }
  try {
    const checkout = ownedOpen(root, dirs, 'directory');
    const parent = ownedOpen(`/proc/self/fd/${checkout}/.ci-artifacts`, dirs, 'directory');
    const run = ownedOpen(`/proc/self/fd/${parent}/${runId}`, dirs, 'directory');
    const identity = ownedOpen(`/proc/self/fd/${run}/identity.json`, files, 'file');
    afterOpen();
    fchmodSync(identity, 0o644);
    fchmodSync(run, 0o711);
  } finally {
    for (const fd of opened.reverse()) closeSync(fd);
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  assert.ok(process.argv[2] && process.argv[3], 'checkout root and RUN_ID required');
  exposeSmokeIdentity(process.argv[2], process.argv[3]);
}

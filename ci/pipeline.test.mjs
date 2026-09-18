// @ts-check
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, mkdtempSync, writeFileSync, rmSync, readdirSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';

test('Jenkins delegates qualification, checks main before binding credentials, and always cleans', () => {
  const pipeline = readFileSync('Jenkinsfile', 'utf8');
  assert.ok(pipeline.includes("defaultValue: 'docker'"));
  assert.ok(pipeline.includes("sh './ci/qualify.sh'"));
  assert.ok(pipeline.indexOf('release-preflight') < pipeline.indexOf('withCredentials'));
  assert.equal(pipeline.match(/when \{ branch 'main' \}/g)?.length, 2);
  assert.ok(pipeline.includes("sh './ci/qualify.sh cleanup'"));
  assert.ok(pipeline.includes('finally { deleteDir() }'));
  assert.doesNotMatch(pipeline, /changeset:version|catchError|docker \{ image/);
});

test('qualification gates are ordered, unfiltered, and cannot publish', () => {
  const script = readFileSync('ci/qualify.sh', 'utf8');
  const commands = ['npm ci', 'node ci/artifacts.mjs clean', 'npm run build -- --force', 'npm run check -- --force',
    'npm test -- --force', 'npm run test:consumer', 'npm run test:integration', 'docker build --no-cache'];
  let previous = -1;
  for (const command of commands) {
    const index = script.indexOf(command);
    assert.ok(index > previous, command); previous = index;
  }
  assert.doesNotMatch(script, /docker login|docker push|changeset:publish|--testNamePattern/);
});

test('consumer failure prints child diagnostics and removes only its own portable scratch', () => {
  const scratch = mkdtempSync(join(tmpdir(), 'consumer-portability-'));
  try {
    const bin = join(scratch, 'bin'); mkdirSync(bin);
    writeFileSync(join(bin, 'npm'), '#!/usr/bin/env node\nconsole.log("synthetic pack stdout"); console.error("synthetic pack stderr"); process.exit(42);\n', { mode: 0o755 });
    writeFileSync(join(scratch, 'unrelated'), 'preserve');
    const result = spawnSync(process.execPath, [resolve('packages/tapeworm_dispatcher_mdb_rmq/test/consumer.mjs')], {
      env: { ...process.env, TMPDIR: scratch, PATH: `${bin}:${process.env.PATH}` }, encoding: 'utf8', timeout: 10000,
    });
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /synthetic pack stdout/);
    assert.match(result.stderr, /synthetic pack stderr/);
    assert.deepEqual(readdirSync(scratch).sort(), ['bin', 'unrelated']);
  } finally { rmSync(scratch, { recursive: true, force: true }); }
});

test('publication config is temporary, literal-token-based and never versions source', () => {
  const release = readFileSync('ci/release.sh', 'utf8');
  assert.ok(release.includes('set +x'));
  assert.ok(release.includes("'//registry.npmjs.org/:_authToken=${NPM_TOKEN}'"));
  assert.ok(release.includes('mktemp -d /tmp/tapeworm-release-'));
  assert.ok(release.includes("trap 'rm -rf -- \"$SECRET_DIR\"' EXIT"));
  assert.doesNotMatch(release, /changeset:version|git commit|git push/);
});

// @ts-check
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, mkdtempSync, writeFileSync, rmSync, readdirSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';

test('Jenkins qualifies all contexts but gates preflight and publication on master', () => {
  const pipeline = readFileSync('Jenkinsfile', 'utf8');
  assert.ok(pipeline.includes("params.DOCKER_AGENT_LABEL ?: 'lynx'"));
  assert.ok(pipeline.includes("defaultValue: 'lynx'"));
  assert.ok(pipeline.includes("sh './ci/qualify.sh'"));
  assert.ok(pipeline.indexOf('release-preflight') < pipeline.indexOf('withCredentials'));
  assert.equal(pipeline.match(/branch 'master'/g)?.length, 2);
  assert.equal(pipeline.match(/not \{ buildingTag\(\) \}/g)?.length, 2);
  assert.doesNotMatch(pipeline, /branch 'main'/);
  assert.ok(pipeline.includes("sh './ci/qualify.sh cleanup'"));
  assert.ok(pipeline.includes('finally { deleteDir() }'));
  assert.doesNotMatch(pipeline, /changeset:version|catchError|docker \{ image/);
});

test('only the master publication stage receives credentials', () => {
  const pipeline = readFileSync('Jenkinsfile', 'utf8');
  const preflight = pipeline.indexOf("stage('Release preflight without credentials')");
  const publish = pipeline.indexOf("stage('Publish qualified artifacts')");
  const credentials = pipeline.indexOf('withCredentials');
  assert.ok(preflight >= 0 && publish > preflight && credentials > publish);
  assert.equal(pipeline.match(/withCredentials/g)?.length, 1);
  assert.doesNotMatch(pipeline.slice(preflight, publish), /withCredentials|credentialsId/);
  for (const stage of [pipeline.slice(preflight, publish), pipeline.slice(publish)]) {
    assert.match(stage, /branch 'master'[\s\S]*not \{ buildingTag\(\) \}/);
  }
  assert.match(pipeline.slice(publish), /not \{ buildingTag\(\) \}[\s\S]*withCredentials/);
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
    const result = spawnSync(process.execPath, [resolve('packages/tapeworm_dispatcher_mdb_rmq/test/consumer/run.mjs')], {
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
  assert.ok(release.includes('-e BRANCH_NAME -e TAG_NAME'));
  assert.doesNotMatch(release, /changeset:version|git commit|git push/);
});

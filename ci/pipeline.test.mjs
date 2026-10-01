// @ts-check
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, mkdtempSync, writeFileSync, rmSync, readdirSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';

test('PR validation targets develop and ignores description edits', () => {
  const pipeline = readFileSync('.github/workflows/pr.yml', 'utf8');
  assert.match(pipeline, /pull_request:\s+branches: \[develop\]\s+types: \[opened, synchronize, reopened\]/);
  for (const command of ['npm ci', 'npm run build', 'npm run check', 'npm test']) {
    assert.ok(pipeline.includes(`run: ${command}`));
  }
  assert.doesNotMatch(pipeline, /secrets\.|changeset:publish|pull_request_target|edited/);
  assert.match(pipeline, /cancel-in-progress: true/);
});

test('develop pushes version or publish checked packages and publish the dispatcher image', () => {
  const pipeline = readFileSync('.github/workflows/publish.yml', 'utf8');
  assert.match(pipeline, /on:\s+push:\s+branches: \[develop\]/);
  assert.match(pipeline, /cancel-in-progress: false/);
  let previous = -1;
  for (const command of ['npm ci', 'npm run build', 'npm run check', 'npm test', 'npm run changeset:publish', 'docker/login-action', 'docker/build-push-action']) {
    const index = pipeline.indexOf(command);
    assert.ok(index > previous, command); previous = index;
  }
  assert.doesNotMatch(pipeline.slice(0, pipeline.indexOf('- name: Version or publish packages')), /secrets\./);
  assert.match(pipeline, /uses: changesets\/action@v2/);
  assert.match(pipeline, /version-script: npm run changeset:version/);
  assert.match(pipeline, /publish-script: npm run changeset:publish/);
  assert.match(pipeline, /pr-base-branch: develop/);
  assert.match(pipeline, /pull-requests: write/);
  assert.match(pipeline, /push-git-tags: true/);
  assert.match(pipeline, /create-github-releases: true/);
  const docker = pipeline.slice(pipeline.indexOf('\n  docker:'));
  assert.match(docker, /needs: publish/);
  assert.match(docker, /uses: actions\/checkout@/);
  assert.doesNotMatch(docker, /if:|changesets\/action/);
  assert.match(pipeline, /file: packages\/tapeworm_dispatcher_mdb_rmq\/Dockerfile/);
  assert.match(pipeline, /push: true/);
  assert.match(pipeline, /IMAGE: surikaterna\/tapeworm-dispatcher/);
  assert.match(pipeline, /bash ci\/docker-tags.sh/);
  assert.match(pipeline, /tags: \$\{\{ steps.image-tags.outputs.tags \}\}/);
  assert.doesNotMatch(pipeline, /continue-on-error|ci\/qualify.sh/);
});

test('Docker tags preserve existing versions, add missing versions, and reject lookup errors', () => {
  const scratch = mkdtempSync(join(tmpdir(), 'tapeworm-docker-tags-'));
  try {
    writeFileSync(join(scratch, 'docker'), `#!/usr/bin/env bash
set -eu
[[ "$*" == 'manifest inspect docker.io/surikaterna/tapeworm-dispatcher:1.0.0' ]]
printf '%s\\n' "$INSPECT_RESULT" >&2
exit "$INSPECT_STATUS"
`, { mode: 0o755 });
    const baseTags = 'tags<<EOF\nsurikaterna/tapeworm-dispatcher:develop-42\nsurikaterna/tapeworm-dispatcher:latest\n';
    for (const scenario of [
      { status: '0', result: 'existing manifest', version: false, success: true },
      { status: '1', result: 'no such manifest: docker.io/surikaterna/tapeworm-dispatcher:1.0.0', version: true, success: true },
      { status: '1', result: 'no such manifest: docker.io/another/image:1.0.0', version: false, success: false },
      { status: '1', result: 'unauthorized: authentication required', version: false, success: false },
      { status: '1', result: 'dial tcp: connection refused', version: false, success: false },
    ]) {
      const result = spawnSync('bash', ['ci/docker-tags.sh'], {
        env: {
          ...process.env, PATH: `${scratch}:${process.env.PATH}`,
          IMAGE: 'surikaterna/tapeworm-dispatcher', VERSION: '1.0.0', RUN_NUMBER: '42',
          INSPECT_STATUS: scenario.status, INSPECT_RESULT: scenario.result,
        },
        encoding: 'utf8', timeout: 10000,
      });
      assert.equal(result.status, scenario.success ? 0 : 1, result.stderr);
      assert.equal(result.stdout, scenario.success
        ? `${baseTags}${scenario.version ? 'surikaterna/tapeworm-dispatcher:1.0.0\n' : ''}EOF\n`
        : '');
    }
  } finally { rmSync(scratch, { recursive: true, force: true }); }
});

test('qualification gates are ordered, unfiltered, and cannot publish', () => {
  const script = readFileSync('ci/qualify.sh', 'utf8');
  const commands = ['npm ci', 'node ci/artifacts.mjs clean', 'npm run build -- --force', 'npm run check -- --force',
    'npm test -- --force', 'npm run test:consumer', 'npm run test:integration', '"$DOCKER_BIN" build --no-cache'];
  let previous = -1;
  for (const command of commands) {
    const index = script.indexOf(command);
    assert.ok(index > previous, command); previous = index;
  }
  assert.doesNotMatch(script, /login| push|changeset:publish|--testNamePattern/);
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

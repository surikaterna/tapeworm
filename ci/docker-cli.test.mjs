// @ts-check
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

const scripts = ['ci/qualify.sh', 'ci/services.sh', 'ci/release.sh', 'ci/cleanup.test.sh'];

function executable(path, body) {
  writeFileSync(path, `#!/usr/bin/env bash\nset -euo pipefail\n${body}\n`, { mode: 0o755 });
}

function cleanupWith(env, runId) {
  const result = spawnSync('bash', ['ci/qualify.sh', 'cleanup'], {
    env: { ...env, RUN_ID: runId }, encoding: 'utf8', timeout: 10000,
  });
  rmSync(join('.ci-artifacts', runId), { recursive: true, force: true });
  return result;
}

test('local default resolves docker from PATH and routes every cleanup query through it', () => {
  const scratch = mkdtempSync(join(tmpdir(), 'tapeworm-docker-default-'));
  try {
    const bin = join(scratch, 'bin');
    const log = join(scratch, 'calls');
    mkdirSync(bin);
    executable(join(bin, 'docker'), 'printf "%s\\n" "$*" >> "$DOCKER_LOG"');
    const env = { ...process.env, PATH: `${bin}:${process.env.PATH}`, DOCKER_LOG: log };
    delete env.DOCKER_BIN;
    const result = cleanupWith(env, 'docker-default-test');
    assert.equal(result.status, 0, result.stderr);
    assert.deepEqual(readFileSync(log, 'utf8').trim().split('\n').slice(-3), [
      'ps -aq --filter label=org.tapeworm.ci.run=docker-default-test --filter label=org.tapeworm.ci.project=qualification',
      'network ls -q --filter label=org.tapeworm.ci.run=docker-default-test --filter label=org.tapeworm.ci.project=qualification',
      'volume ls -q --filter label=org.tapeworm.ci.run=docker-default-test --filter label=org.tapeworm.ci.project=qualification',
    ]);
  } finally { rmSync(scratch, { recursive: true, force: true }); }
});

test('explicit Docker path wins over PATH and supports spaces', () => {
  const scratch = mkdtempSync(join(tmpdir(), 'tapeworm-docker-override-'));
  try {
    const bin = join(scratch, 'bin');
    const selected = join(scratch, 'selected docker cli');
    const log = join(scratch, 'selected-calls');
    mkdirSync(bin);
    executable(join(bin, 'docker'), 'exit 91');
    executable(selected, 'printf "%s\\n" "$*" >> "$DOCKER_LOG"');
    const result = cleanupWith({
      ...process.env, PATH: `${bin}:${process.env.PATH}`, DOCKER_BIN: selected, DOCKER_LOG: log,
    }, 'docker-override-test');
    assert.equal(result.status, 0, result.stderr);
    assert.deepEqual(readFileSync(log, 'utf8').trim().split('\n').slice(-3).map((call) => call.split(' ')[0]), [
      'ps', 'network', 'volume',
    ]);
  } finally { rmSync(scratch, { recursive: true, force: true }); }
});

test('missing explicit Docker selection fails closed with corrective guidance', () => {
  const scratch = mkdtempSync(join(tmpdir(), 'tapeworm-docker-missing-'));
  try {
    const result = cleanupWith({ ...process.env, DOCKER_BIN: join(scratch, 'missing docker cli') }, 'docker-missing-test');
    assert.equal(result.status, 127);
    assert.match(result.stderr, /Docker CLI unavailable: DOCKER_BIN=.*missing docker cli/);
    assert.match(result.stderr, /Set DOCKER_BIN to an executable Docker CLI path or command/);
  } finally { rmSync(scratch, { recursive: true, force: true }); }
});

test('all host entry points validate selection and contain no bare Docker execution', () => {
  for (const path of scripts) {
    const source = readFileSync(path, 'utf8');
    assert.ok(source.includes(': "${DOCKER_BIN:=docker}"'), path);
    assert.ok(source.includes('command -v -- "$DOCKER_BIN"'), path);
    assert.ok(source.includes('export DOCKER_BIN'), path);
    assert.doesNotMatch(source, /(^|[\s;&|()])docker(?=\s)/m, path);
    assert.doesNotMatch(source, /^\s*eval\b/m, path);
  }

  const qualify = readFileSync('ci/qualify.sh', 'utf8');
  const cleanup = readFileSync('ci/cleanup.test.sh', 'utf8');
  const jenkins = readFileSync('Jenkinsfile', 'utf8');
  assert.match(qualify, /export DOCKER_BIN[\s\S]*ci\/release\.sh[\s\S]*ci\/services\.sh/);
  assert.match(cleanup, /DOCKER_BIN="\$SCRATCH\/docker" RUN_ID="\$FAIL" \.\/ci\/qualify\.sh/);
  assert.doesNotMatch(cleanup, /PATH="\$SCRATCH:/);
  assert.doesNotMatch(jenkins, /DOCKER_BIN|DOCKER_AGENT_LABEL/);
  assert.ok(jenkins.includes("agent { label 'lynx' }"));
  assert.ok(jenkins.includes("sh './ci/qualify.sh cleanup'"));
});

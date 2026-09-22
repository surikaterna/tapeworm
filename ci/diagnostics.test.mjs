// @ts-check
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

function executable(path, body) {
  writeFileSync(path, `#!/usr/bin/env bash\n${body}\n`, { mode: 0o755 });
}

function run(script, args, env) {
  return spawnSync('bash', [script, ...args], { env, encoding: 'utf8', timeout: 10000 });
}

function fakeDocker(path) {
  executable(path, `
printf '%s\\n' "$*" >> "$DOCKER_LOG"
printf '%s\\n' 'https://private-user:private-password@daemon.invalid' >&2
case "\${1:-}" in
  version) printf '%s\\n' 'client=27.1 server=27.1' ; exit "\${VERSION_STATUS:-0}" ;;
  context) printf '%s\\n' "\${CONTEXT_RESULT:-lynx-safe}" ; exit "\${CONTEXT_STATUS:-0}" ;;
  info) printf '%s\\n' "\${OSTYPE_RESULT:-linux}" ; exit "\${INFO_STATUS:-0}" ;;
  ps) exit "\${CLEANUP_STATUS:-0}" ;;
esac
exit 0
`);
}

test('diagnostics resolve the default once, allowlist output, and never mutate Docker', () => {
  const scratch = mkdtempSync(join(tmpdir(), 'tapeworm-diagnostics-default-'));
  try {
    const bin = join(scratch, 'bin'); const log = join(scratch, 'calls');
    mkdirSync(bin); fakeDocker(join(bin, 'docker'));
    const env = {
      ...process.env, PATH: `${bin}:${bin}:/usr/bin:/bin`, DOCKER_LOG: log,
      NODE_NAME: 'lynx-agent', NODE_LABELS: 'linux lynx', WORKSPACE: '/safe/work space',
      DOCKER_HOST: 'tcp://secret-endpoint.invalid', DOCKER_CONFIG: '/secret/docker-config',
      DOCKER_REGISTRY: 'secret.registry.invalid', NPM_TOKEN: 'private-npm-token',
      CONTEXT_RESULT: 'https://context-secret.invalid',
    };
    delete env.DOCKER_BIN;
    const result = run('ci/diagnostics.sh', ['qualification'], env);
    assert.equal(result.status, 0, result.stderr);
    assert.match(result.stdout, /DIAGNOSTIC docker_requested=docker/);
    assert.match(result.stdout, /DIAGNOSTIC linux_docker_capability=PASS exit_status=0/);
    const candidates = [...result.stdout.matchAll(/docker_path_candidate=(.*)/g)].map((match) => match[1]);
    assert.equal(new Set(candidates).size, candidates.length);
    assert.equal(candidates.filter((candidate) => candidate.includes(scratch)).length, 1);
    assert.ok(result.stdout.includes('docker_context_name=\\[redacted-unexpected-output\\]'));
    for (const secret of ['secret-endpoint', 'docker-config', 'secret.registry', 'private-npm-token', 'private-password', 'context-secret']) {
      assert.doesNotMatch(result.stdout + result.stderr, new RegExp(secret));
    }
    assert.deepEqual(readFileSync(log, 'utf8').trim().split('\n').map((line) => line.split(' ')[0]), [
      'version', 'context', 'info',
    ]);
  } finally { rmSync(scratch, { recursive: true, force: true }); }
});

test('an explicit Docker path containing spaces wins over PATH', () => {
  const scratch = mkdtempSync(join(tmpdir(), 'tapeworm-diagnostics-override-'));
  try {
    const bin = join(scratch, 'bin'); const selected = join(scratch, 'selected docker');
    const log = join(scratch, 'calls'); mkdirSync(bin);
    executable(join(bin, 'docker'), 'exit 91'); fakeDocker(selected);
    const result = run('ci/diagnostics.sh', [], {
      ...process.env, PATH: `${bin}:/usr/bin:/bin`, DOCKER_BIN: selected, DOCKER_LOG: log,
    });
    assert.equal(result.status, 0, result.stderr);
    assert.match(result.stdout, /docker_selected=.*selected\\ docker/);
    assert.equal(readFileSync(log, 'utf8').trim().split('\n').length, 3);
  } finally { rmSync(scratch, { recursive: true, force: true }); }
});

test('missing and broken Docker selections are reported nonfatally', () => {
  const scratch = mkdtempSync(join(tmpdir(), 'tapeworm-diagnostics-missing-'));
  try {
    const missing = join(scratch, 'missing'); const broken = join(scratch, 'broken');
    symlinkSync(missing, broken);
    for (const selected of [missing, broken]) {
      const result = run('ci/diagnostics.sh', ['cleanup'], {
        ...process.env, PATH: '/usr/bin:/bin', DOCKER_BIN: selected,
      });
      assert.equal(result.status, 0, result.stderr);
      assert.match(result.stdout, /linux_docker_capability=FAIL exit_status=127/);
    }
    const brokenResult = run('ci/diagnostics.sh', ['cleanup'], {
      ...process.env, PATH: '/usr/bin:/bin', DOCKER_BIN: broken,
    });
    assert.match(brokenResult.stdout, /docker_symlink_target=.*missing/);
  } finally { rmSync(scratch, { recursive: true, force: true }); }
});

test('non-Linux daemon fails hard qualification with actionable guidance', () => {
  const scratch = mkdtempSync(join(tmpdir(), 'tapeworm-diagnostics-ostype-'));
  const runId = `diagnostics-non-linux-${scratch.split('/').at(-1)}`;
  try {
    const docker = join(scratch, 'docker'); const log = join(scratch, 'calls'); fakeDocker(docker);
    const result = run('ci/qualify.sh', [], {
      ...process.env, DOCKER_BIN: docker, DOCKER_LOG: log, RUN_ID: runId, OSTYPE_RESULT: 'windows',
    });
    assert.equal(result.status, 1, result.stderr);
    assert.match(result.stdout, /linux_docker_capability=FAIL exit_status=0/);
    assert.match(result.stdout + result.stderr, /a Linux daemon is required/);
  } finally {
    rmSync(join('.ci-artifacts', runId), { recursive: true, force: true });
    rmSync(scratch, { recursive: true, force: true });
  }
});

test('cleanup diagnostics stay nonfatal while cleanup preserves Docker status', () => {
  const scratch = mkdtempSync(join(tmpdir(), 'tapeworm-diagnostics-cleanup-'));
  const runId = `diagnostics-cleanup-${scratch.split('/').at(-1)}`;
  try {
    const docker = join(scratch, 'docker'); const log = join(scratch, 'calls'); fakeDocker(docker);
    const result = run('ci/qualify.sh', ['cleanup'], {
      ...process.env, DOCKER_BIN: docker, DOCKER_LOG: log, RUN_ID: runId,
      INFO_STATUS: '55', VERSION_STATUS: '54', CONTEXT_STATUS: '53', CLEANUP_STATUS: '37',
    });
    assert.equal(result.status, 37, result.stderr);
    assert.match(result.stdout, /DIAGNOSTIC phase=cleanup/);
    assert.match(result.stdout, /linux_docker_capability=FAIL exit_status=55/);
  } finally {
    rmSync(join('.ci-artifacts', runId), { recursive: true, force: true });
    rmSync(scratch, { recursive: true, force: true });
  }
});

test('authoritative capability probe preserves Docker failure status', () => {
  const scratch = mkdtempSync(join(tmpdir(), 'tapeworm-diagnostics-status-'));
  const runId = `diagnostics-status-${scratch.split('/').at(-1)}`;
  try {
    const docker = join(scratch, 'docker'); const log = join(scratch, 'calls'); fakeDocker(docker);
    const result = run('ci/qualify.sh', [], {
      ...process.env, DOCKER_BIN: docker, DOCKER_LOG: log, RUN_ID: runId, INFO_STATUS: '42',
    });
    assert.equal(result.status, 42, result.stderr);
    assert.match(result.stdout + result.stderr, /capability check failed \(exit 42\).*endpoint details are intentionally suppressed/);
  } finally {
    rmSync(join('.ci-artifacts', runId), { recursive: true, force: true });
    rmSync(scratch, { recursive: true, force: true });
  }
});

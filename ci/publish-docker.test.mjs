// @ts-check
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFileSync, spawnSync } from 'node:child_process';

const imageId = `sha256:${'a'.repeat(64)}`;
const image = 'surikaterna/tapeworm-dispatcher';

function fixture() {
  const scratch = mkdtempSync(join(tmpdir(), 'tapeworm-local-publish-'));
  const root = join(scratch, 'repo with spaces');
  const log = join(scratch, 'calls');
  const docker = join(scratch, 'fake docker');
  mkdirSync(join(root, 'ci'), { recursive: true });
  for (const file of ['publish-docker.sh', 'docker-tags.sh']) {
    copyFileSync(join('ci', file), join(root, 'ci', file));
  }
  writeFileSync(join(root, '.gitignore'), '.ci-artifacts/\n');
  writeFileSync(join(root, 'package.json'), '{"version":"1.0.0"}\n');
  writeFileSync(join(root, 'ci/qualify.sh'), `#!/usr/bin/env bash
set -euo pipefail
printf 'qualify\\n' >> "$DOCKER_LOG"
[[ "$RUN_ID" != reused ]]
if [[ \${QUALIFY_MODE:-} == fail ]]; then exit 19; fi
art=".ci-artifacts/$RUN_ID"
mkdir -p "$art"
printf '%s\\n' '${imageId}' > "$art/image-id"
git rev-parse HEAD > "$art/revision"
if [[ \${QUALIFY_MODE:-} != missing ]]; then
  printf '%s\\n' '${imageId}' > "$art/qualified-image-id"
fi
case \${QUALIFY_MODE:-} in
  dirty) printf 'changed\\n' >> package.json ;;
  branch) git switch -c changed ;;
  revision) git commit --allow-empty -m changed ;;
  receipt) printf 'wrong\\n' > "$art/revision" ;;
esac
`);
  writeFileSync(docker, `#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" >> "$DOCKER_LOG"
case "$1 $2" in
  'image inspect')
    [[ "$5" == '${imageId}' ]]
    if [[ "$4" == *image.revision* ]]; then
      printf '%s\\n' "\${LABEL_REVISION:-$TEST_REVISION}"
    else
      printf '1.0.0\\n'
    fi ;;
  'manifest inspect')
    case \${LOOKUP:-existing} in
      existing) printf '{}\\n' ;;
      missing) printf 'no such manifest: %s\\n' "$3" >&2; exit 1 ;;
      error) printf 'unauthorized: authentication required\\n' >&2; exit 1 ;;
    esac ;;
  tag*) ;;
  push*) if [[ "$2" == \${FAIL_PUSH:-} ]]; then exit 23; fi ;;
  *) exit 97 ;;
esac
`, { mode: 0o755 });
  const env = {
    ...process.env, DOCKER_BIN: docker, DOCKER_LOG: log,
    GIT_AUTHOR_NAME: 'Test', GIT_AUTHOR_EMAIL: 'test@example.invalid',
    GIT_COMMITTER_NAME: 'Test', GIT_COMMITTER_EMAIL: 'test@example.invalid',
  };
  /** @param {string[]} args */
  const git = (args) => execFileSync('git', args, { cwd: root, env, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
  git(['init', '-b', 'develop']);
  git(['add', '.']);
  git(['commit', '-m', 'fixture']);
  const revision = git(['rev-parse', 'HEAD']);
  return {
    root, git,
    calls: () => existsSync(log) ? readFileSync(log, 'utf8').trim().split('\n') : [],
    /** @param {NodeJS.ProcessEnv} [extra] */
    run: (extra = {}) => spawnSync('bash', [join(root, 'ci/publish-docker.sh')], {
      cwd: scratch, env: { ...env, RUN_ID: 'reused', TEST_REVISION: revision, ...extra }, encoding: 'utf8', timeout: 10000,
    }),
    cleanup: () => rmSync(scratch, { recursive: true, force: true }),
  };
}

for (const state of ['branch', 'detached', 'staged', 'unstaged', 'untracked']) {
  test(`local publication rejects ${state} checkout before qualification or Docker`, () => {
    const f = fixture();
    try {
      if (state === 'branch') f.git(['switch', '-c', 'feature']);
      else if (state === 'detached') f.git(['checkout', '--detach']);
      else if (state === 'untracked') writeFileSync(join(f.root, 'untracked'), 'new');
      else {
        writeFileSync(join(f.root, 'package.json'), '{"version":"2.0.0"}');
        if (state === 'staged') f.git(['add', 'package.json']);
      }
      const result = f.run();
      assert.equal(result.status, 1, result.stderr);
      assert.match(result.stderr, /requires a (develop|clean) checkout/);
      assert.deepEqual(f.calls(), []);
    } finally { f.cleanup(); }
  });
}

for (const mode of ['fail', 'missing', 'dirty', 'branch', 'revision', 'receipt']) {
  test(`local publication refuses failed or changed qualification: ${mode}`, () => {
    const f = fixture();
    try {
      const result = f.run({ QUALIFY_MODE: mode });
      assert.notEqual(result.status, 0, result.stderr);
      assert.deepEqual(f.calls(), ['qualify']);
    } finally { f.cleanup(); }
  });
}

for (const lookup of ['existing', 'missing']) {
  test(`local publication pushes the exact qualified image with ${lookup} version tag`, () => {
    const f = fixture();
    try {
      const result = f.run({ LOOKUP: lookup });
      assert.equal(result.status, 0, result.stderr);
      const calls = f.calls();
      assert.equal(calls[0], 'qualify');
      assert.ok(calls.includes(`manifest inspect docker.io/${image}:1.0.0`));
      const tags = lookup === 'missing' ? [`${image}:1.0.0`, `${image}:latest`] : [`${image}:latest`];
      assert.deepEqual(calls.filter(call => /^(tag|push) /.test(call)),
        tags.flatMap(tag => [`tag ${imageId} ${tag}`, `push ${tag}`]));
      assert.doesNotMatch(calls.join('\n'), /develop-|npm|login|build/);
    } finally { f.cleanup(); }
  });
}

test('lookup failures and image revision mismatches prevent all pushes', () => {
  for (const env of [{ LOOKUP: 'error' }, { LABEL_REVISION: 'wrong' }]) {
    const f = fixture();
    try {
      const result = f.run(env);
      assert.notEqual(result.status, 0);
      assert.ok(f.calls().every(call => !/^(tag|push) /.test(call)));
    } finally { f.cleanup(); }
  }
});

test('a failed version push stops before updating latest', () => {
  const f = fixture();
  try {
    const result = f.run({ LOOKUP: 'missing', FAIL_PUSH: `${image}:1.0.0` });
    assert.equal(result.status, 23, result.stderr);
    assert.deepEqual(f.calls().filter(call => call.startsWith('push ')), [`push ${image}:1.0.0`]);
  } finally { f.cleanup(); }
});

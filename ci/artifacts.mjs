// @ts-check
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, lstatSync, readFileSync, readdirSync, realpathSync, rmSync } from 'node:fs';
import { join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export const runtimePackages = ['tapeworm', 'tapeworm_dispatcher_mdb_rmq'];
/** @typedef {{path: string, bytes: number, sha256: string}} BuiltFile */
/** @param {unknown} value @returns {Record<string, unknown>} */
export function record(value) {
  assert.ok(value && typeof value === 'object' && !Array.isArray(value));
  return Object.fromEntries(Object.entries(value));
}
/** @param {string} file */
export const json = file => record(JSON.parse(readFileSync(file, 'utf8')));
/** @param {string} directory */
function ownedDirectory(directory) {
  assert.ok(lstatSync(directory).isDirectory(), `Not an owned directory: ${directory}`);
  assert.equal(realpathSync(directory), directory, `Symlinked directory: ${directory}`);
}

/** @param {string} root */
export function workspaces(root) {
  ownedDirectory(root);
  assert.deepEqual(json(join(root, 'package.json')).workspaces, ['packages/*']);
  const packages = join(root, 'packages'); ownedDirectory(packages);
  return readdirSync(packages).sort().map(name => {
    assert.match(name, /^[a-zA-Z0-9_-]+$/);
    const directory = join(packages, name); ownedDirectory(directory);
    assert.equal(json(join(directory, 'package.json')).name, name);
    return directory;
  });
}

/** @param {Record<string, unknown>} config */
function noIncrementalState(config) {
  const options = record(config.compilerOptions);
  assert.notEqual(options.incremental, true, 'Incremental output needs an explicit cleanup contract');
  assert.notEqual(options.composite, true, 'Composite output needs an explicit cleanup contract');
  assert.equal(options.tsBuildInfoFile, undefined);
  return options;
}

/** @param {string} root */
export function declaredOutputs(root) {
  const packages = workspaces(root);
  const base = json(join(root, 'tsconfig.base.json'));
  assert.equal(base.extends, undefined); noIncrementalState(base);
  return packages.flatMap(directory => {
    const config = json(join(directory, 'tsconfig.json'));
    assert.equal(config.extends, '../../tsconfig.base.json');
    assert.equal(noIncrementalState(config).outDir, 'dist');
    const outputs = [join(directory, 'dist')];
    const worker = join(directory, 'tsconfig.worker.json');
    if (existsSync(worker)) {
      const workerConfig = json(worker);
      assert.equal(workerConfig.extends, './tsconfig.json');
      assert.equal(noIncrementalState(workerConfig).outDir, 'dist-worker');
      outputs.push(join(directory, 'dist-worker'));
    }
    return outputs;
  });
}

/** Validate every deletion before removing anything; never follow output symlinks.
 * @param {string} root */
export function cleanOutputs(root) {
  const outputs = declaredOutputs(root);
  const tracked = execFileSync('git', ['ls-files', '-z', '--', ...outputs.map(path => relative(root, path))],
    { cwd: root, encoding: 'utf8' });
  assert.equal(tracked, '', 'Refusing to clean tracked files in generated outputs');
  for (const output of outputs) {
    const stat = lstatSync(output, { throwIfNoEntry: false });
    assert.ok(!stat || stat.isDirectory(), `Refusing non-directory/symlink output: ${output}`);
  }
  for (const output of outputs) rmSync(output, { recursive: true, force: true });
  console.log('Pristine generated outputs', outputs.map(path => relative(root, path)));
}

/** @param {string} directory @param {string} [prefix] @returns {BuiltFile[]} */
export function inventory(directory, prefix = '') {
  ownedDirectory(directory);
  return readdirSync(directory, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name)).flatMap(entry => {
    const file = join(directory, entry.name);
    const path = `${prefix}${entry.name}`;
    if (entry.isDirectory()) return inventory(file, `${path}/`);
    assert.ok(entry.isFile(), `Non-regular artifact: ${file}`);
    const bytes = readFileSync(file);
    return [{ path, bytes: bytes.length, sha256: createHash('sha256').update(bytes).digest('hex') }];
  });
}

/** @param {string} root */
export function runtimeBuilds(root) {
  return Object.fromEntries(runtimePackages.map(name => [name, inventory(join(root, 'packages', name, 'dist'))]));
}

/** @param {unknown} expected @param {ReturnType<typeof runtimeBuilds>} actual */
export function sameRuntimeBuilds(expected, actual) {
  assert.deepEqual(actual, expected, 'Runtime artifact inventory/bytes changed');
}

/** @param {string} root */
export function allBuildHash(root) {
  const builds = workspaces(root).map(directory => [relative(root, directory), inventory(join(directory, 'dist'))]);
  return createHash('sha256').update(JSON.stringify(builds)).digest('hex');
}

/** @param {unknown} value */
function packFiles(value) {
  assert.ok(Array.isArray(value) && value.length === 1, 'Expected one npm pack result');
  const files = record(value[0]).files;
  assert.ok(Array.isArray(files));
  return files.map((/** @type {unknown} */ entry) => {
    const file = record(entry);
    assert.ok(typeof file.path === 'string' && typeof file.size === 'number');
    return { path: file.path, bytes: file.size };
  }).filter(file => file.path.startsWith('dist/')).sort((a, b) => a.path.localeCompare(b.path));
}

/** @param {string} root */
export function verifyNpmRuntimeFiles(root) {
  for (const name of runtimePackages) {
    const directory = join(root, 'packages', name);
    const output = execFileSync('npm', ['pack', '--dry-run', '--json', '--ignore-scripts'], { cwd: directory, encoding: 'utf8' });
    const expected = inventory(join(directory, 'dist')).map(file => ({ path: `dist/${file.path}`, bytes: file.bytes }))
      .sort((a, b) => a.path.localeCompare(b.path));
    assert.deepEqual(packFiles(JSON.parse(output)), expected, `npm runtime inventory mismatch: ${name}`);
    console.log('NPM runtime inventory matches built files', name, expected.length);
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  assert.equal(process.argv[2], 'clean');
  cleanOutputs(process.cwd());
}

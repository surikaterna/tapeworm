import { execFileSync } from 'node:child_process';
import { mkdtempSync, copyFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

// An actual pack/install boundary: no TypeScript workspace aliases or source paths.
const root = resolve(fileURLToPath(new URL('../../../', import.meta.url)));
const scratch = mkdtempSync('/tmp/opencode/gqxm-consumer-');
const packs = join(scratch, 'packs');
mkdirSync(packs);
/** @param {string[]} args @param {string} [cwd] */
const npm = (args, cwd = scratch) => execFileSync('npm', args, { cwd, encoding: 'utf8' });
/** @param {string} directory */
const pack = (directory) => {
  const output = npm(['pack', '--json', '--pack-destination', packs], directory);
  /** @type {unknown} */
  const parsed = JSON.parse(output);
  if (!Array.isArray(parsed)) throw new Error('Invalid npm pack result');
  /** @type {unknown} */
  const first = parsed[0];
  if (!first || typeof first !== 'object' || !('filename' in first) || typeof first.filename !== 'string') {
    throw new Error('Invalid npm pack filename');
  }
  return join(packs, first.filename);
};
const core = pack(join(root, 'tapeworm'));
const dispatcher = pack(join(root, 'tapeworm_dispatcher_mdb_rmq'));
writeFileSync(join(scratch, 'package.json'), JSON.stringify({ private: true, type: 'module' }));
npm(['install', '--ignore-scripts', core, dispatcher, 'typescript@6.0.2', '@types/amqplib@0.10.8', '@types/node@26.5.1']);
copyFileSync(new URL('./fixture.ts', import.meta.url), join(scratch, 'consumer.ts'));
writeFileSync(join(scratch, 'tsconfig.json'), JSON.stringify({ compilerOptions: { target: 'ES2022', module: 'NodeNext',
  moduleResolution: 'NodeNext', types: ['node'], strict: true, noUncheckedIndexedAccess: true, skipLibCheck: false, noEmit: true }, include: ['consumer.ts'] }));
process.stdout.write(npm(['exec', '--', 'tsc', '-p', 'tsconfig.json']));
process.stdout.write(`Packed consumer passed: ${scratch}\n`);

import { defineConfig } from 'vitest/config';
import path from 'node:path';

export default defineConfig({
  test: {
    globals: true,
    include: ['test/**/*.test.ts']
  },
  resolve: {
    alias: {
      tapeworm: path.resolve(__dirname, '../tapeworm/index.ts'),
      tapeworm_persistence_store_indexeddb: path.resolve(__dirname, '../tapeworm_persistence_store_indexeddb')
    }
  }
});

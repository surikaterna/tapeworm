import tseslint from 'typescript-eslint';
export default tseslint.config(
  ...tseslint.configs.strictTypeChecked,
  { languageOptions: { parserOptions: { project: './tsconfig.test.json', tsconfigRootDir: import.meta.dirname } },
    rules: { '@typescript-eslint/no-non-null-assertion': 'error',
      '@typescript-eslint/restrict-template-expressions': ['error', { allowNumber: true }],
      'max-lines': ['error', 400], 'max-lines-per-function': ['error', { max: 49 }],
      'max-depth': ['error', 3] } },
);

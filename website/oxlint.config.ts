import { defineConfig } from '@apify/oxlint-config';

export default defineConfig({
    plugins: ['react'],
    ignorePatterns: [
        '**/node_modules',
        '.docusaurus',
        'build',
        'versioned_docs',
        'versioned_sidebars',
        'api-typedoc-generated.json',
        'module_shortcuts.json',
    ],
    rules: {
        // Docusaurus theme/plugin/page modules are loaded via their default export.
        'import/no-default-export': 'off',
        // Site code logs intentionally (plugin diagnostics, build-time helpers).
        'no-console': 'off',
    },
});

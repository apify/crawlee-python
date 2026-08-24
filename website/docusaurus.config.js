/* eslint-disable global-require */
const fs = require('node:fs');
const path = require('node:path');

const { externalLinkProcessor } = require('./tools/utils/externalLink');

const GROUP_ORDER = [
    'Autoscaling',
    'Browser management',
    'Configuration',
    'Crawlers',
    'Crawling contexts',
    'Errors',
    'Event data',
    'Event managers',
    'Functions',
    'HTTP clients',
    'HTTP parsers',
    'Request loaders',
    'Session management',
    'Statistics',
    'Storage clients',
    'Storage data',
    'Storages',
    'Other',
];

const groupSort = (g1, g2) => {
    if (GROUP_ORDER.includes(g1) && GROUP_ORDER.includes(g2)) {
        return GROUP_ORDER.indexOf(g1) - GROUP_ORDER.indexOf(g2);
    }
    return g1.localeCompare(g2);
};

/** @type {Partial<import('@docusaurus/types').DocusaurusConfig>} */
module.exports = {
    title: 'Crawlee for Python · Fast, reliable Python web crawlers.',
    url: 'https://crawlee.dev',
    baseUrl: '/python/',
    trailingSlash: false,
    organizationName: 'apify',
    projectName: 'crawlee-python',
    scripts: ['/python/js/custom.js', '/crawlee-python/js/custom.js'],
    githubHost: 'github.com',
    future: {
        faster: true,
        v4: {
            removeLegacyPostBuildHeadAttribute: true,
            useCssCascadeLayers: false, // this breaks styles on homepage and link colors everywhere
        },
    },
    headTags: [
        // Intercom messenger
        {
            tagName: 'script',
            innerHTML: `window.intercomSettings={api_base:"https://api-iam.intercom.io",app_id:"kod1r788"};`,
            attributes: {},
        },
        // Intercom messenger
        {
            tagName: 'script',
            innerHTML: `(function(){var w=window;var ic=w.Intercom;if(typeof ic==="function"){ic('reattach_activator');ic('update',w.intercomSettings);}else{var d=document;var i=function(){i.c(arguments);};i.q=[];i.c=function(args){i.q.push(args);};w.Intercom=i;var l=function(){var s=d.createElement('script');s.type='text/javascript';s.async=true;s.src='https://widget.intercom.io/widget/kod1r788';var x=d.getElementsByTagName('script')[0];x.parentNode.insertBefore(s,x);};if(document.readyState==='complete'){l();}else if(w.attachEvent){w.attachEvent('onload',l);}else{w.addEventListener('load',l,false);}}})()`,
            attributes: {},
        },
    ],
    favicon: 'img/favicon.ico',
    customFields: {
        markdownOptions: {
            html: true,
        },
        gaGtag: true,
        repoUrl: 'https://github.com/apify/crawlee-python',
    },
    onBrokenLinks: 'throw',
    markdown: {
        mermaid: true,
        hooks: {
            onBrokenMarkdownLinks: 'throw',
        },
    },
    themes: ['@docusaurus/theme-mermaid'],
    presets: /** @type {import('@docusaurus/types').PresetConfig[]} */ ([
        [
            '@docusaurus/preset-classic',
            /** @type {import('@docusaurus/preset-classic').Options} */
            ({
                docs: {
                    showLastUpdateAuthor: true,
                    showLastUpdateTime: true,
                    path: '../docs',
                    sidebarPath: './sidebars.js',
                    rehypePlugins: [externalLinkProcessor],
                    editUrl: (doc) => {
                        return `https://github.com/apify/crawlee-python/edit/master/website/${doc.versionDocsDirPath}/${doc.docPath}`;
                    },
                },
                theme: {
                    customCss: '/src/css/custom.css',
                },
            }),
        ],
    ]),
    plugins: [
        [
            '@apify/docusaurus-plugin-typedoc-api',
            {
                projectRoot: '.',
                changelogs: false,
                readmes: false,
                packages: [{ path: '.' }],
                typedocOptions: {
                    excludeExternals: false,
                },
                sortSidebar: groupSort,
                routeBasePath: 'api',
                python: true,
                pythonOptions: {
                    pythonModulePath: path.join(__dirname, '../src/crawlee'),
                    moduleShortcutsPath: path.join(__dirname, 'module_shortcuts.json'),
                },
            },
        ],
        [
            '@docusaurus/plugin-client-redirects',
            {
                createRedirects(existingPath) {
                    // Maps a current docs path suffix to the old suffixes that should redirect
                    // to it, covering the restructuring that introduced the Concepts section
                    // and merged the Examples section into Concepts and Guides. Redirects are
                    // derived from existing routes, so they apply to every docs version that
                    // contains the new page and never shadow versions that still have the old
                    // layout (see OLD_STRUCTURE_VERSIONS below).
                    const MOVED_DOCS = {
                        'concepts/architecture-overview': ['guides/architecture-overview'],
                        'concepts/http-crawlers': [
                            'guides/http-crawlers',
                            'examples/beautifulsoup-crawler',
                            'examples/parsel-crawler',
                            'examples/file-download',
                        ],
                        'concepts/playwright-crawler': [
                            'guides/playwright-crawler',
                            'examples/playwright-crawler',
                            'examples/playwright-crawler-with-block-requests',
                            'examples/capture-screenshots-using-playwright',
                        ],
                        'concepts/adaptive-playwright-crawler': [
                            'guides/adaptive-playwright-crawler',
                            'examples/adaptive-playwright-crawler',
                        ],
                        'concepts/request-router': ['guides/request-router'],
                        'concepts/request-loaders': ['guides/request-loaders', 'examples/using-sitemap-request-loader'],
                        'concepts/storages': [
                            'guides/storages',
                            'examples/add-data-to-dataset',
                            'examples/export-entire-dataset-to-file',
                        ],
                        'concepts/storage-clients': ['guides/storage-clients'],
                        'concepts/http-clients': ['guides/http-clients'],
                        'concepts/session-management': ['guides/session-management'],
                        'concepts/cookie-management': ['guides/cookie-management'],
                        'concepts/http-headers': ['guides/http-headers'],
                        'concepts/proxy-management': ['guides/proxy-management'],
                        'concepts/scaling-crawlers': ['guides/scaling-crawlers'],
                        'concepts/request-throttling': ['guides/request-throttling'],
                        'concepts/error-handling': [
                            'guides/error-handling',
                            'examples/capturing-page-snapshots-with-error-snapshotter',
                        ],
                        'concepts/logging': ['examples/configure-json-logging'],
                        'concepts/service-locator': ['guides/service-locator'],
                        'guides/crawling-links': [
                            'examples/crawl-all-links-on-website',
                            'examples/crawl-multiple-urls',
                            'examples/crawl-specific-links-on-website',
                            'examples/crawl-website-with-relative-links',
                        ],
                        'guides/fill-and-submit-web-form': ['examples/fill-and-submit-web-form'],
                        'guides/respect-robots-txt-file': ['examples/respect-robots-txt-file'],
                        'guides/stopping-and-resuming-crawlers': [
                            'examples/crawler-stop',
                            'examples/crawler-keep-alive',
                            'examples/resuming-paused-crawl',
                        ],
                        'guides/run-parallel-crawlers': ['examples/run-parallel-crawlers'],
                        'guides/avoid-blocking': [
                            'examples/playwright-crawler-with-camoufox',
                            'examples/playwright-crawler-with-fingerprint-generator',
                        ],
                        'guides/logging-in-with-a-crawler': ['examples/using_browser_profile'],
                        guides: ['examples'],
                    };

                    // Doc versions that still have the pre-restructure layout. Their routes
                    // (e.g. /docs/1.9/examples/...) must not get redirects generated over them.
                    const OLD_STRUCTURE_VERSIONS = ['0.6', '1.9'];
                    const versions = require('./versions.json');
                    const latestHasOldStructure = OLD_STRUCTURE_VERSIONS.includes(versions[0]);

                    const marker = '/docs/';
                    const markerIndex = existingPath.indexOf(marker);

                    if (markerIndex === -1) {
                        return undefined;
                    }

                    const redirects = [];

                    for (const [currentSuffix, oldSuffixes] of Object.entries(MOVED_DOCS)) {
                        if (!existingPath.endsWith(`/${currentSuffix}`)) {
                            continue;
                        }

                        // E.g. '/python/docs/', '/python/docs/next/' or '/python/docs/1.9/'.
                        const docsRoot = existingPath.slice(0, existingPath.length - currentSuffix.length);
                        const version = docsRoot.slice(markerIndex + marker.length).replace(/\/$/, '');
                        const hasOldStructure =
                            version === '' ? latestHasOldStructure : OLD_STRUCTURE_VERSIONS.includes(version);

                        if (!hasOldStructure) {
                            redirects.push(...oldSuffixes.map((oldSuffix) => `${docsRoot}${oldSuffix}`));
                        }
                    }

                    return redirects.length > 0 ? redirects : undefined;
                },
            },
        ],
        [
            'docusaurus-gtm-plugin',
            {
                id: 'GTM-5P7MCS7',
            },
        ],
        [
            '@signalwire/docusaurus-plugin-llms-txt',
            {
                enableDescriptions: false,
                content: {
                    includeVersionedDocs: false,
                    enableLlmsFullTxt: true,
                    relativePaths: false,
                    excludeRoutes: [
                        '/python/api/[0-9]*/**',
                        '/python/api/[0-9]*',
                        '/python/api/next/**',
                        '/python/api/next',
                    ],
                },
            },
        ],
        async function runnableCodeBlock() {
            return {
                name: 'runnable-code-block',
                configureWebpack() {
                    return {
                        resolveLoader: {
                            alias: {
                                'roa-loader': require.resolve(`${__dirname}/roa-loader/`),
                            },
                        },
                    };
                },
            };
        },
        // skipping svgo for animated crawlee logo
        async function doNotUseSVGO() {
            return {
                name: 'docusaurus-svgo',
                configureWebpack(config) {
                    // find the svg rule
                    const svgRule = config.module.rules.find(
                        (r) => typeof r === 'object' && r.test.toString() === '/\\.svg$/i',
                    );

                    // find the svgr loader
                    const svgrLoader = svgRule?.oneOf?.[0];

                    // make copy of svgr loader and disable svgo
                    const svgrLoaderCopy = JSON.parse(JSON.stringify(svgrLoader));

                    // include only animated logo
                    svgrLoaderCopy.include = /animated-crawlee-logo/;

                    // turn off svgo
                    svgrLoaderCopy.use[0].options.svgo = false;

                    // insert the copy after the original svgr loader
                    svgRule.oneOf.splice(1, 0, svgrLoaderCopy);

                    // exclude animated logo from the first svgr loader (with svgo enabled)
                    svgrLoader.exclude = /animated-crawlee-logo/;

                    return {
                        mergeStrategy: {
                            'module.rules': 'replace',
                        },
                        module: {
                            rules: config.module.rules,
                        },
                    };
                },
            };
        },
        // Copy root CHANGELOG.md to docs/ and all versioned_docs/ so every
        // doc version displays the same (latest) changelog — not a snapshot.
        function changelogFromRoot() {
            const sourceChangelog = path.join(__dirname, '..', 'CHANGELOG.md');
            return {
                name: 'changelog-from-root',
                async loadContent() {
                    if (!fs.existsSync(sourceChangelog)) return;

                    const changelog = fs.readFileSync(sourceChangelog, 'utf-8');
                    const docsDir = path.join(__dirname, '..', 'docs');
                    const versionedDocsDir = path.join(__dirname, 'versioned_docs');

                    const targetDirs = [docsDir];
                    if (fs.existsSync(versionedDocsDir)) {
                        for (const version of fs.readdirSync(versionedDocsDir)) {
                            targetDirs.push(path.join(versionedDocsDir, version));
                        }
                    }

                    for (const dir of targetDirs) {
                        fs.writeFileSync(path.join(dir, 'changelog.md'), changelog);
                    }
                },
                getPathsToWatch() {
                    return [sourceChangelog];
                },
            };
        },
        [
            path.resolve(__dirname, 'src/plugins/docusaurus-plugin-segment'),
            {
                writeKey: process.env.SEGMENT_TOKEN,
                allowedInDev: false,
            },
        ],
    ],
    themeConfig: /** @type {import('@docusaurus/preset-classic').ThemeConfig} */ ({
        docs: {
            versionPersistence: 'localStorage',
            sidebar: {
                hideable: true,
            },
        },
        navbar: {
            hideOnScroll: true,
            logo: {
                src: 'img/crawlee-python-light.svg',
                srcDark: 'img/crawlee-python-dark.svg',
            },
            title: 'Crawlee for Python',
            items: [
                {
                    type: 'doc',
                    docId: 'quick-start/quick-start',
                    label: 'Docs',
                    position: 'left',
                },
                {
                    type: 'custom-api',
                    label: 'API',
                    position: 'left',
                    activeBaseRegex: 'api/(?!.*/changelog)',
                },
                {
                    type: 'doc',
                    label: 'Changelog',
                    docId: 'changelog',
                    className: 'changelog',
                },
                {
                    href: 'https://crawlee.dev/blog',
                    target: '_self',
                    rel: 'dofollow',
                    label: 'Blog',
                    position: 'left',
                },
                {
                    type: 'docsVersionDropdown',
                    position: 'right',
                    dropdownItemsBefore: [],
                    dropdownItemsAfter: [],
                },
            ],
        },
        colorMode: {
            defaultMode: 'light',
            disableSwitch: false,
            respectPrefersColorScheme: true,
        },
        prism: {
            defaultLanguage: 'typescript',
            theme: require('prism-react-renderer').themes.github,
            darkTheme: require('prism-react-renderer').themes.dracula,
            additionalLanguages: ['docker', 'log', 'bash', 'diff', 'json'],
        },
        metadata: [
            // eslint-disable-next-line max-len
            {
                name: 'description',
                content: `Crawlee helps you build and maintain your Python crawlers. It's open source and modern, with type hints for Python to help you catch bugs early.`,
            },
            // eslint-disable-next-line max-len
            {
                name: 'og:description',
                content: `Crawlee helps you build and maintain your Python crawlers. It's open source and modern, with type hints for Python to help you catch bugs early.`,
            },
        ],
        image: 'img/crawlee-python-og.png',
        footer: {
            links: [
                {
                    title: 'Docs',
                    items: [
                        {
                            label: 'Quick start',
                            to: 'docs/quick-start',
                        },
                        {
                            label: 'Guides',
                            to: 'docs/guides',
                        },
                        {
                            label: 'API reference',
                            to: 'api',
                        },
                        {
                            label: 'Changelog',
                            to: 'docs/changelog',
                        },
                    ],
                },
                {
                    title: 'Product',
                    items: [
                        {
                            label: 'Discord',
                            href: 'https://discord.com/invite/jyEM2PRvMU',
                        },
                        {
                            label: 'Stack Overflow',
                            href: 'https://stackoverflow.com/questions/tagged/crawlee-python',
                        },
                        {
                            label: 'Twitter',
                            href: 'https://twitter.com/apify',
                        },
                        {
                            label: 'YouTube',
                            href: 'https://www.youtube.com/apify',
                        },
                    ],
                },
                {
                    title: 'More',
                    items: [
                        {
                            label: 'Apify platform',
                            href: 'https://apify.com',
                        },
                        {
                            label: 'Docusaurus',
                            href: 'https://docusaurus.io',
                        },
                        {
                            label: 'GitHub',
                            href: 'https://github.com/apify/crawlee-python',
                        },
                    ],
                },
            ],
        },
        algolia: {
            appId: '5JC94MPMLY',
            apiKey: '878493fcd7001e3c179b6db6796a999b', // search only (public) API key
            indexName: 'crawlee_python',
            placeholder: 'Search documentation',
            algoliaOptions: {
                facetFilters: ['version:VERSION'],
            },
            translations: {
                button: {
                    buttonText: 'Search documentation...',
                },
            },
        },
    }),
};

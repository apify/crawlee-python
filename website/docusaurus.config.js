/* eslint-disable global-require */
const path = require('path');

const { externalLinkProcessor } = require('./tools/utils/externalLink');

const GROUP_ORDER = [
    'Classes',
    'Abstract classes',
    'Data structures',
    'Errors',
    'Functions',
    'Constructors',
    'Methods',
    'Properties',
    'Constants',
    'Enumeration Members',
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
    scripts: [
        '/python/js/custom.js',
        '/crawlee-python/js/custom.js',
    ],
    githubHost: 'github.com',
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
    markdown: {
        mermaid: true,
    },
    themes: [
        '@docusaurus/theme-mermaid',
    ],
    onBrokenLinks:
    /** @type {import('@docusaurus/types').ReportingSeverity} */ ('throw'),
    onBrokenMarkdownLinks:
    /** @type {import('@docusaurus/types').ReportingSeverity} */ ('throw'),
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
                    // disableVersioning: true,
                    editUrl: (doc) => {
                        return `https://github.com/apify/crawlee-python/edit/master/website/${doc.versionDocsDirPath}/${doc.docPath}`;
                    },
                },
                // blog: {
                //     blogTitle: 'Crawlee Blog - learn how to build better scrapers',
                //     blogDescription: 'Guides and tutorials on using Crawlee, the most reliable open-source web scraping and browser automation library for JavaScript and Node.js developers.',
                // },
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
                pythonOptions: {
                    pythonModulePath: path.join(__dirname, '../src/crawlee'),
                    moduleShortcutsPath: path.join(__dirname, 'module_shortcuts.json'),
                },
            },
        ],
        // [
        //     '@docusaurus/plugin-client-redirects',
        //     {
        //         redirects: [
        //             {
        //                 from: '/docs',
        //                 to: '/docs/quick-start',
        //             },
        //             {
        //                 from: '/docs/next',
        //                 to: '/docs/next/quick-start',
        //             },
        //             {
        //                 from: '/docs/guides/environment-variables',
        //                 to: '/docs/guides/configuration',
        //             },
        //             {
        //                 from: '/docs/guides/getting-started',
        //                 to: '/docs/introduction',
        //             },
        //             {
        //                 from: '/docs/guides/apify-platform',
        //                 to: '/docs/deployment/apify-platform',
        //             },
        //         ],
        //         createRedirects(existingPath) {
        //             if (!existingPath.endsWith('/')) {
        //                 return `${existingPath}/`;
        //             }
        //
        //             return undefined; // Return a falsy value: no redirect created
        //         },
        //     },
        // ],
        [
            'docusaurus-gtm-plugin',
            {
                id: 'GTM-5P7MCS7',
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
    ],
    themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */ ({
        docs: {
            versionPersistence: 'localStorage',
            sidebar: {
                hideable: true,
            },
        },
        announcementBar: {
            id: 'announcement-bar-',
            content: `🎉️ <b>If you like Crawlee for Python, <a href="https://github.com/apify/crawlee-python/">star us on GitHub!</a></b> 🥳️`,
        },
        navbar: {
            hideOnScroll: true,
            title: 'Crawlee for Python',
            logo: {
                src: 'img/crawlee-light-new.svg',
                srcDark: 'img/crawlee-dark-new.svg',
            },
            items: [
                {
                    type: 'doc',
                    docId: 'quick-start/quick-start',
                    label: 'Docs',
                    position: 'left',
                },
                {
                    type: 'doc',
                    docId: '/examples',
                    label: 'Examples',
                    position: 'left',
                },
                {
                    to: '/api',
                    label: 'API',
                    position: 'left',
                    activeBaseRegex: 'api/(?!.*/changelog)',
                },
                // {
                //     type: 'custom-api',
                //     to: 'core/changelog',
                //     label: 'Changelog',
                //     position: 'left',
                //     className: 'changelog',
                //     activeBaseRegex: 'changelog',
                // },
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
                    type: 'dropdown',
                    label: 'Python',
                    position: 'left',
                    items: [
                        {
                            label: 'Node.js',
                            href: 'https://crawlee.dev',
                            target: '_self',
                            rel: 'dofollow',
                        },
                        {
                            label: 'Python',
                            href: '#',
                            target: '_self',
                            rel: 'dofollow',
                        },
                    ],
                },
                // {
                //     type: 'docsVersionDropdown',
                //     position: 'left',
                //     dropdownItemsAfter: [
                //         {
                //             href: 'https://sdk.apify.com/docs/guides/getting-started',
                //             label: '2.2',
                //         },
                //         {
                //             href: 'https://sdk.apify.com/docs/1.3.1/guides/getting-started',
                //             label: '1.3',
                //         },
                //     ],
                // },
                {
                    href: 'https://github.com/apify/crawlee-python',
                    label: 'GitHub',
                    title: 'View on GitHub',
                    position: 'right',
                    className: 'icon',
                },
                {
                    href: 'https://discord.com/invite/jyEM2PRvMU',
                    label: 'Discord',
                    title: 'Chat on Discord',
                    position: 'right',
                    className: 'icon',
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
            { name: 'description', content: `Crawlee helps you build and maintain your Python crawlers. It's open source and modern, with type hints for Python to help you catch bugs early.` },
            // eslint-disable-next-line max-len
            { name: 'og:description', content: `Crawlee helps you build and maintain your Python crawlers. It's open source and modern, with type hints for Python to help you catch bugs early.` },
        ],
        image: 'img/crawlee-og.png',
        footer: {
            links: [
                {
                    title: 'Docs',
                    items: [
                        // {
                        //     label: 'Guides',
                        //     to: 'docs/guides',
                        // },
                        {
                            label: 'Examples',
                            to: 'docs/examples',
                        },
                        {
                            label: 'API reference',
                            to: 'api',
                        },
                        // {
                        //     label: 'Upgrading to v3',
                        //     to: 'docs/upgrading/upgrading-to-v3',
                        // },
                    ],
                },
                {
                    title: 'Community',
                    items: [
                        {
                            label: 'Blog',
                            href: 'https://crawlee.dev/blog',
                            // to: 'blog',
                        },
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
            logo: {
                src: 'img/apify_logo.svg',
                href: '/',
                width: '60px',
                height: '60px',
            },
        },
        algolia: {
            appId: '5JC94MPMLY',
            apiKey: '878493fcd7001e3c179b6db6796a999b', // search only (public) API key
            indexName: 'crawlee_python',
            algoliaOptions: {
                facetFilters: ['version:VERSION'],
            },
        },
    }),
};

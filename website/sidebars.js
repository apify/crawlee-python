module.exports = {
    docs: [
        'quick-start/quick-start',
        {
            type: 'category',
            label: 'Introduction',
            collapsed: false,
            link: {
                type: 'doc',
                id: 'introduction/introduction',
            },
            items: [
                'introduction/setting-up',
                // 'introduction/first-crawler',
                // 'introduction/adding-urls',
                // 'introduction/real-world-project',
                // 'introduction/crawling',
                // 'introduction/scraping',
                // 'introduction/saving-data',
                // 'introduction/refactoring',
                // 'introduction/deployment',
            ],
        },
        // {
        //     type: 'category',
        //     label: 'Guides',
        //     link: {
        //         type: 'generated-index',
        //         title: 'Guides',
        //         slug: '/guides',
        //         keywords: ['guides'],
        //     },
        //     items: [
        //         'guides/request-storage',
        //         'guides/result-storage',
        //         'guides/configuration',
        //         'guides/cheerio-crawler-guide',
        //         'guides/javascript-rendering',
        //         'guides/proxy-management',
        //         'guides/session-management',
        //         'guides/scaling-crawlers',
        //         'guides/avoid-blocking',
        //         'guides/jsdom-crawler-guide',
        //         'guides/got-scraping',
        //         'guides/typescript-project',
        //         'guides/docker-images',
        //         'guides/parallel-scraping/parallel-scraping-guide',
        //     ],
        // },
        // {
        //     type: 'category',
        //     label: 'Deployment',
        //     link: {
        //         type: 'generated-index',
        //         title: 'Deployment guides',
        //         description: 'Here you can find guides on how to deploy your crawlers to various cloud providers.',
        //         slug: '/deployment',
        //     },
        //     items: [
        //         {
        //             type: 'doc',
        //             id: 'deployment/apify-platform',
        //             label: 'Deploy on Apify',
        //         },
        //         {
        //             type: 'category',
        //             label: 'Deploy on AWS',
        //             items: [
        //                 'deployment/aws-cheerio',
        //                 'deployment/aws-browsers',
        //             ],
        //         },
        //         {
        //             type: 'category',
        //             label: 'Deploy to Google Cloud',
        //             items: [
        //                 'deployment/gcp-cheerio',
        //                 'deployment/gcp-browsers',
        //             ],
        //         },
        //     ],
        // },
        {
            type: 'category',
            label: 'Examples',
            link: {
                type: 'generated-index',
                title: 'Examples',
                slug: '/examples',
                keywords: ['examples'],
            },
            items: [
                {
                    type: 'autogenerated',
                    dirName: 'examples',
                },
            ],
        },
        // {
        //     type: 'category',
        //     label: 'Experiments',
        //     link: {
        //         type: 'generated-index',
        //         title: 'Experiments',
        //         slug: '/experiments',
        //         keywords: ['experiments', 'experimental-features'],
        //     },
        //     items: [
        //         {
        //             type: 'autogenerated',
        //             dirName: 'experiments',
        //         },
        //     ],
        // },
        // {
        //     type: 'category',
        //     label: 'Upgrading',
        //     link: {
        //         type: 'generated-index',
        //         title: 'Upgrading',
        //         slug: '/upgrading',
        //         keywords: ['upgrading'],
        //     },
        //     items: [
        //         {
        //             type: 'autogenerated',
        //             dirName: 'upgrading',
        //         },
        //     ],
        // },
    ],
};

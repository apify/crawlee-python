/* eslint-disable max-len */
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import CodeBlock from '@theme/CodeBlock';
import Layout from '@theme/Layout';
import ThemedImage from '@theme/ThemedImage';
import React from 'react';

import styles from './index.module.css';
import Button from '../components/Button';
import HomepageCliExample from '../components/Homepage/HomepageCliExample';
import HomepageCtaSection from '../components/Homepage/HomepageCtaSection';
import HomepageHeroSection from '../components/Homepage/HomepageHeroSection';
import LanguageInfoWidget from '../components/Homepage/LanguageInfoWidget';
import RiverSection from '../components/Homepage/RiverSection';
import ThreeCardsWithIcon from '../components/Homepage/ThreeCardsWithIcon';
import RunnableCodeBlock from '../components/RunnableCodeBlock';

function GetStartedSection() {
    return (
        <section className={styles.styles}>
            <LanguageInfoWidget
                language="Python"
                githubUrl="https://github.com/apify/crawlee-python"
                to="/python/docs/quick-start"
            />
        </section>
    );
}

const example = `import asyncio

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext


async def main() -> None:
    crawler = PlaywrightCrawler(
        max_requests_per_crawl=5,  # Limit the crawl to 5 requests at most.
        headless=False,  # Show the browser window.
        browser_type='firefox',  # Use the Firefox browser.
    )

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Extract and enqueue all links found on the page.
        await context.enqueue_links()

        # Extract data from the page using Playwright API.
        data = {
            'url': context.request.url,
            'title': await context.page.title(),
            'content': (await context.page.content())[:100],
        }

        # Push the extracted data to the default dataset.
        await context.push_data(data)

    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://crawlee.dev'])

    # Export the entire dataset to a JSON file.
    await crawler.export_data('results.json')

    # Or work with the data directly.
    data = await crawler.get_data()
    crawler.log.info(f'Extracted data: {data.items}')


if __name__ == '__main__':
    asyncio.run(main())
`;

function CodeExampleSection() {
    return (
        <section className={styles.codeExampleSection}>
            <div className={styles.decorativeRow} />
            <div className={styles.codeBlockContainer}>
                <RunnableCodeBlock className={styles.codeBlock}>
                    {{
                        code: example,
                        hash: '',
                    }}
                </RunnableCodeBlock>
            </div>
            <div className={styles.dashedSeparator} />
            <div className={styles.decorativeRow} />
        </section>
    );
}

const benefitsCodeBlockCrawler = `{
    useFingerprints: true,
    fingerprintOptions: {
        fingerprintGeneratorOptions: {
            browsers: ['chrome', 'firefox'],
            devices: ['mobile'],
            locales: ['en-US'],
        },
    },
},
`;

const benefitsCodeBlockHeadless = `const crawler = new AdaptivePlaywrightCrawler({
    renderingTypeDetectionRatio: 0.1,
    async requestHandler({ querySelector, enqueueLinks }) {
        // The crawler detects if JS rendering is needed
        // to extract this data. If not, it will use HTTP
        // for follow-up requests to save time and costs.
        const $prices = await querySelector('span.price')
        await enqueueLinks();
    },
});
`;

function BenefitsSection() {
    return (
        <section className={styles.benefitsSection}>
            <h2>What are the benefits?</h2>
            <RiverSection
                title="Unblock websites by default"
                description="Crawlee crawls stealthily with zero configuration, but you can customize its behavior to overcome any protection. Real-world fingerprints included."
                content={
                    <CodeBlock className="code-block" language="python">
                        {benefitsCodeBlockCrawler}
                    </CodeBlock>
                }
                to="/docs/guides/avoid-blocking"
            />
            <div className={styles.trianglesSeparator} />
            <RiverSection
                title="Work with your favorite tools"
                description="Crawlee integrates BeautifulSoup, Cheerio, Puppeteer, Playwright, and other popular open-source tools. No need to learn new syntax."
                content={
                    <ThemedImage
                        alt="Work with your favorite tools"
                        sources={{
                            light: 'img/py-schema-light.webp',
                            dark: 'img/py-schema-dark.webp',
                        }}
                    />
                }
                reversed
                to="/docs/quick-start#choose-your-crawler"
            />
            <div className={styles.trianglesSeparator} />
            <RiverSection
                title="One API for headless and HTTP"
                description="Switch between HTTP and headless without big rewrites thanks to a shared API. Or even let Adaptive crawler decide if JS rendering is needed."
                content={
                    <CodeBlock className="code-block" language="python">
                        {benefitsCodeBlockHeadless}
                    </CodeBlock>
                }
                to="/api"
            />
        </section>
    );
}

function OtherFeaturesSection() {
    return (
        <section className={styles.otherFeaturesSection}>
            <h2>What else is in Crawlee?</h2>
            <div className={styles.cardsWithContentContainer}>
                <div className={styles.cardsWithImageContainer}>
                    <div className={styles.cardWithImage}>
                        <ThemedImage
                            sources={{
                                light: 'img/auto-scaling-light.webp',
                                dark: 'img/auto-scaling-dark.webp',
                            }}
                            alt=""
                        />
                        <div className={styles.cardWithImageText}>
                            <h3 className={styles.cardWithImageTitle}>
                                Auto scaling
                            </h3>
                            <div className={styles.cardWithImageDescription}>
                                Crawlers automatically adjust concurrency based
                                on available system resources. Avoid memory
                                errors in small containers and run faster in
                                large ones.
                            </div>
                        </div>
                    </div>
                    <div className={styles.cardWithImage}>
                        <ThemedImage
                            sources={{
                                light: 'img/smart-proxy-light.webp',
                                dark: 'img/smart-proxy-dark.webp',
                            }}
                            alt=""
                        />
                        <div className={styles.cardWithImageText}>
                            <h3 className={styles.cardWithImageTitle}>
                                Smart proxy rotation
                            </h3>
                            <div className={styles.cardWithImageDescription}>
                                Crawlee uses a pool of sessions represented by
                                different proxies to maintain the proxy
                                performance and keep IPs healthy. Blocked
                                proxies are removed from the pool automatically.
                            </div>
                        </div>
                    </div>
                </div>
                <ThreeCardsWithIcon
                    cards={[
                        {
                            icon: (
                                <ThemedImage
                                    sources={{
                                        light: 'img/queue-light-icon.svg',
                                        dark: 'img/queue-dark-icon.svg',
                                    }}
                                    alt=""
                                />
                            ),
                            title: 'Queue and storage',
                            description:
                                'Pause and resume crawlers thanks to a persistent queue of URLs and storage for structured data.',
                        },
                        {
                            icon: (
                                <ThemedImage
                                    sources={{
                                        light: 'img/scraping-utils-light-icon.svg',
                                        dark: 'img/scraping-utils-dark-icon.svg',
                                    }}
                                    alt=""
                                />
                            ),
                            title: 'Handy scraping utils',
                            description:
                                'Sitemaps, infinite scroll, contact extraction, large asset blocking and many more utils included.',
                        },
                        {
                            icon: (
                                <ThemedImage
                                    sources={{
                                        light: 'img/routing-light-icon.svg',
                                        dark: 'img/routing-dark-icon.svg',
                                    }}
                                    alt=""
                                />
                            ),
                            title: 'Routing & middleware',
                            description:
                                'Keep your code clean and organized while managing complex crawls with a built-in router that streamlines the process.',
                        },
                    ]}
                />
            </div>
        </section>
    );
}

function DeployToCloudSection() {
    return (
        <section className={styles.deployToCloudSection}>
            <div className={styles.deployToCloudLeftSide}>
                <h2>Deploy to cloud </h2>
                <div className={styles.deployToCloudDescription}>
                    Crawlee, by Apify, works anywhere, but Apify offers the best
                    experience. Easily turn your project into an Actor—a
                    serverless micro-app with built-in infra, proxies, and
                    storage.
                </div>
                <Button
                    withIcon
                    to="https://docs.apify.com/platform/actors/development/deployment"
                >
                    Deploy to Apify
                </Button>
            </div>
            <div className={styles.deployToCloudRightSide}>
                <div
                    className={styles.dashedSeparatorVertical}
                    id={styles.verticalStepLine}
                />
                <div className={styles.deployToCloudStep}>
                    <div className={styles.deployToCloudStepNumber}>
                        <div>1</div>
                    </div>
                    <div className={styles.deployToCloudStepText}>
                        Install Apify SDK and Apify CLI.
                    </div>
                </div>
                <div className={styles.deployToCloudStep}>
                    <div className={styles.deployToCloudStepNumber}>
                        <div>2</div>
                    </div>
                    <div className={styles.deployToCloudStepText}>
                        Add <pre>Actor.init()</pre> to the begining and{' '}
                        <pre>Actor.exit()</pre> to the end of your code.
                    </div>
                </div>
                <div className={styles.deployToCloudStep}>
                    <div className={styles.deployToCloudStepNumber}>
                        <div>3</div>
                    </div>
                    <div className={styles.deployToCloudStepText}>
                        Use the Apify CLI to push the code to the Apify
                        platform.
                    </div>
                </div>
            </div>
        </section>
    );
}

function BuildFastScrapersSection() {
    return (
        <section className={styles.buildFastScrapersSection}>
            <div className={styles.dashedDecorativeCircle} />
            <div className={styles.dashedSeparator} />
            <h2>Crawlee helps you build scrapers faster</h2>
            <ThreeCardsWithIcon
                cards={[
                    {
                        icon: (
                            <ThemedImage
                                sources={{
                                    light: 'img/zero-setup-light-icon.svg',
                                    dark: 'img/zero-setup-dark-icon.svg',
                                }}
                                alt=""
                            />
                        ),
                        title: 'Zero setup required',
                        description:
                            'Use on the templates, install Crawlee and go. No CLI required, no complex file structure, no boilerplate.',
                        actionLink: {
                            text: 'Get started',
                            href: '/docs/quick-start',
                        },
                    },
                    {
                        icon: (
                            <ThemedImage
                                sources={{
                                    light: 'img/defaults-light-icon.svg',
                                    dark: 'img/defaults-dark-icon.svg',
                                }}
                                alt=""
                            />
                        ),
                        title: 'Reasonable defaults',
                        description:
                            'Unblocking, proxy rotation and other core features are already turned on. But also very configurable.',
                        actionLink: {
                            text: 'Learn more',
                            href: '/docs/examples',
                        },
                    },
                    {
                        icon: (
                            <ThemedImage
                                sources={{
                                    light: 'img/community-light-icon.svg',
                                    dark: 'img/community-dark-icon.svg',
                                }}
                                alt=""
                            />
                        ),
                        title: 'Helpful community',
                        description:
                            'Join our Discord community of over Xk developers and get fast answers to your web scraping questions.',
                        actionLink: {
                            text: 'Join Discord',
                            href: 'https://discord.gg/jyEM2PRvMU',
                        },
                    },
                ]}
            />
        </section>
    );
}

export default function JavascriptHomepage() {
    const { siteConfig } = useDocusaurusContext();
    return (
        <Layout description={siteConfig.description}>
            <div id={styles.homepageContainer}>
                <HomepageHeroSection />
                <GetStartedSection />
                <div className={styles.dashedSeparator} />
                <CodeExampleSection />
                <HomepageCliExample />
                <div className={styles.dashedSeparator}>
                    <div
                        className={styles.dashedDecorativeCircle}
                        id={styles.ctaDecorativeCircle}
                    />
                </div>
                <BenefitsSection />
                <div className={styles.dashedSeparator} />
                <OtherFeaturesSection />
                <div className={styles.dashedSeparator} />
                <DeployToCloudSection />
                <div className={styles.dashedSeparator} />
                <BuildFastScrapersSection />
                <HomepageCtaSection />
            </div>
        </Layout>
    );
}

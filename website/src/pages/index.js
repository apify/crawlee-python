/* eslint-disable max-len */
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import CodeBlock from '@theme/CodeBlock';
import Layout from '@theme/Layout';
import ThemedImage from '@theme/ThemedImage';
import clsx from 'clsx';
import React from 'react';

import styles from './index.module.css';
import Button from '../components/Button';
import HomepageCliExample from '../components/Homepage/HomepageCliExample';
import HomepageCtaSection from '../components/Homepage/HomepageCtaSection';
import HomepageHeroSection from '../components/Homepage/HomepageHeroSection';
import LanguageInfoWidget from '../components/Homepage/LanguageInfoWidget';
import RiverSection from '../components/Homepage/RiverSection';
import RunnableCodeBlock from '../components/RunnableCodeBlock';
import ThreeCardsWithIcon from '../components/Homepage/ThreeCardsWithIcon';

import HomePageExample from '!!raw-loader!roa-loader!./home_page_example.py';

function GetStartedSection() {
    return (
        <section className={styles.languageGetStartedSection}>
            <LanguageInfoWidget
                language="Python"
                githubUrl="https://github.com/apify/crawlee-python"
                to="/python/docs/quick-start"
            />
        </section>
    );
}

function CodeExampleSection() {
    return (
        <section className={styles.codeExampleSection}>
            <div className={styles.decorativeRow} />
            <div className={styles.codeBlockContainer}>
                <RunnableCodeBlock className="language-python" language="python">
                    {HomePageExample}
                </RunnableCodeBlock>
            </div>
            <div className={styles.dashedSeparator} />
            <div className={styles.decorativeRow} />
        </section>
    );
}

const benefitsCodeBlockCrawler = `fingerprint_generator = DefaultFingerprintGenerator(
    header_options=HeaderGeneratorOptions(
        browsers=['chromium', 'firefox'],
        devices=['mobile'],
        locales=['en-US']
    ),
)`;

// TODO:
const benefitsCodeBlockHeadless = `crawler = AdaptivePlaywrightCrawler.with_parsel_static_parser()

@crawler.router.default_handler
async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
    prices = await context.query_selector_all('span.price')
    await context.enqueue_links()`;

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
                            light: '/python/img/favorite-tools-light.webp',
                            dark: '/python/img/favorite-tools-dark.webp',
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
                    <Link className={styles.cardWithImage} to="/docs/guides/scaling-crawlers">
                        <ThemedImage
                            sources={{
                                light: '/python/img/auto-scaling-light.webp',
                                dark: '/python/img/auto-scaling-dark.webp',
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
                    </Link>
                    <Link className={styles.cardWithImage} to="/docs/guides/proxy-management">
                        <ThemedImage
                            sources={{
                                light: '/python/img/smart-proxy-light.webp',
                                dark: '/python/img/smart-proxy-dark.webp',
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
                    </Link>
                </div>
                <ThreeCardsWithIcon
                    cards={[
                        {
                            icon: (
                                <ThemedImage
                                    sources={{
                                        light: '/python/img/queue-light-icon.svg',
                                        dark: '/python/img/queue-dark-icon.svg',
                                    }}
                                    alt=""
                                />
                            ),
                            title: 'Queue and storage',
                            description:
                                'Pause and resume crawlers thanks to a persistent queue of URLs and storage for structured data.',
                            to: '/docs/guides/storages',
                        },
                        {
                            icon: (
                                <ThemedImage
                                    sources={{
                                        light: '/python/img/scraping-utils-light-icon.svg',
                                        dark: '/python/img/scraping-utils-dark-icon.svg',
                                    }}
                                    alt=""
                                />
                            ),
                            title: 'Handy scraping utils',
                            description:
                                'Sitemaps, infinite scroll, contact extraction, large asset blocking and many more utils included.',
                            to: '/docs/guides/avoid-blocking',

                        },
                        {
                            icon: (
                                <ThemedImage
                                    sources={{
                                        light: '/python/img/routing-light-icon.svg',
                                        dark: '/python/img/routing-dark-icon.svg',
                                    }}
                                    alt=""
                                />
                            ),
                            title: 'Routing & middleware',
                            description:
                                'Keep your code clean and organized while managing complex crawls with a built-in router that streamlines the process.',
                            to: '/api/class/Router',
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
                    experience. Easily turn your project into an{' '}
                    <Link to="https://apify.com/actors" rel="dofollow">
                        Actor
                    </Link>
                    —a serverless micro-app with built-in infra, proxies, and
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
                                    light: '/python/img/zero-setup-light-icon.svg',
                                    dark: '/python/img/zero-setup-dark-icon.svg',
                                }}
                                alt=""
                            />
                        ),
                        title: 'Zero setup required',
                        description:
                            'Copy code example, install Crawlee and go. No CLI required, no complex file structure, no boilerplate.',
                        actionLink: {
                            text: 'Get started',
                            href: '/docs/quick-start',
                        },
                    },
                    {
                        icon: (
                            <ThemedImage
                                sources={{
                                    light: '/python/img/defaults-light-icon.svg',
                                    dark: '/python/img/defaults-dark-icon.svg',
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
                                    light: '/python/img/community-light-icon.svg',
                                    dark: '/python/img/community-dark-icon.svg',
                                }}
                                alt=""
                            />
                        ),
                        title: 'Helpful community',
                        description:
                            'Join our Discord community of over 10k developers and get fast answers to your web scraping questions.',
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
                <div className={clsx(styles.dashedSeparator, styles.codeExampleTopSeparator)} />
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

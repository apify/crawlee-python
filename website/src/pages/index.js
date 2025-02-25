/* eslint-disable max-len */
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Admonition from '@theme/Admonition';
import CodeBlock from '@theme/CodeBlock';
import Layout from '@theme/Layout';
import clsx from 'clsx';
import React from 'react';
import LiteYouTubeEmbed from 'react-lite-youtube-embed';

import styles from './index.module.css';
import Highlights from '../components/Highlights';
import 'react-lite-youtube-embed/dist/LiteYouTubeEmbed.css';
import RunnableCodeBlock from '../components/RunnableCodeBlock';

import HomePageExample from '!!raw-loader!./home_page_example.py';


function Hero() {
    return (
        <header className={clsx('container', styles.heroBanner)}>
            <div className="row padding-horiz--md">
                <div className="col col--7">
                    <div className={clsx(styles.relative, 'row')}>
                        <div className="col">
                            <h1 className={styles.tagline}>
                                Build your Python<br/> web crawlers<br/> using Crawlee
                            </h1>
                            <h1 className={styles.tagline}>
                                Build your <span>Python<br/> web crawlers</span><br/> using <span>Crawlee</span>
                            </h1>
                        </div>
                    </div>
                    <div className="row">
                        <div className="col">
                            <h2>It helps you build reliable Python web crawlers. Fast.</h2>
                        </div>
                    </div>
                    <div className="row">
                        <div className="col">
                            <div className={styles.earlyAdopters}>🚀 Crawlee for Python is open to early adopters!</div>
                        </div>
                    </div>
                    <div className="row">
                        <div className="col">
                            <div className={styles.heroButtons}>
                                <Link to="/docs/introduction" className={styles.getStarted}>Get Started</Link>
                                <iframe src="https://ghbtns.com/github-btn.html?user=apify&repo=crawlee-python&type=star&count=true&size=large" frameBorder="0" scrolling="0" width="170" height="30" title="GitHub"></iframe>
                            </div>
                        </div>
                    </div>
                </div>
                <div className={clsx(styles.relative, 'col', 'col--5')}>
                    <div className={styles.logoBlur}>
                        <img src={require('../../static/img/logo-blur.png').default} className={clsx(styles.hideSmall)} />
                    </div>
                    <div className={styles.codeBlock}>
                        <CodeBlock className="language-bash">
                            pipx run 'crawlee[cli]' create my-crawler
                        </CodeBlock>
                    </div>
                </div>
            </div>
        </header>
    );
}

function Features() {
    return (
        <section className={clsx('container', styles.features)}>
            <div className="row">
                {/*<div className="col col--6">*/}
                <div className="col">
                    <h2>Build reliable Python web crawlers 🏗️</h2>
                    <p>
                        Crawlee won't fix broken selectors for you (yet), but it helps you <b>build and maintain your Python web crawlers faster</b>.
                    </p>
                    <p>
                        When a website adds <a href="https://crawlee.dev/docs/guides/javascript-rendering">JavaScript rendering</a>, you don't have to rewrite everything, only switch to a browser crawler. When you later find a great API to speed up your crawls, flip the switch back.
                    </p>
                    {/*<p>*/}
                    {/*    It keeps your proxies healthy by rotating them smartly with good fingerprints that make your crawlers*/}
                    {/*    look human-like. It's not unblockable,*/}
                    {/*    but <a href="https://blog.apify.com/daltix-python-vs-apify-sdk/" target="_blank" rel="noreferrer"><b>it will save you money in the long run</b></a>.*/}
                    {/*</p>*/}
                    <p>
                        Crawlee is built by people who scrape for a living and use it every day to scrape millions of pages.
                        <a href="https://discord.com/invite/jyEM2PRvMU" target="_blank" rel="noreferrer"><b> Meet our community on Discord</b></a>.
                    </p>
                </div>
                {/*<div className="col col--6">*/}
                {/*    <div className="video-container">*/}
                {/*        <LiteYouTubeEmbed*/}
                {/*            id="g1Ll9OlFwEQ"*/}
                {/*            params="autoplay=1&autohide=1&showinfo=0&rel=0"*/}
                {/*            title="Crawlee, the web scraping and browser automation library"*/}
                {/*            poster="maxresdefault"*/}
                {/*            webp*/}
                {/*        />*/}
                {/*    </div>*/}
                {/*</div>*/}
            </div>
        </section>
    );
}

function ActorExample() {
    return (
        <section className={clsx(styles.try, 'container')}>
            <div className="col">
                <h2>Try Crawlee out 👾</h2>
                <Admonition type="caution" title="before you start">
                    Crawlee requires <a href="https://www.python.org/downloads/" target="_blank" rel="noreferrer"><b>Python 3.9 or higher</b></a>.
                </Admonition>
                <p>
                    The fastest way to try Crawlee out is to use the <b>Crawlee CLI</b> and choose one of the provided templates. The CLI will prepare a new project for you, and add boilerplate code for you to play with.
                </p>
                <CodeBlock className="language-bash">
                    pipx run 'crawlee[cli]' create my-crawler
                </CodeBlock>
                <p>
                    If you prefer to integrate Crawlee <b>into your own project</b>, you can follow the example below. Crawlee is available on  <a href="https://pypi.org/project/crawlee/">PyPI</a>, so you can install it using <code>pip</code>. Since it uses <code>PlaywrightCrawler</code>, you will also need to install <code>crawlee</code> package with <code>playwright</code> extra. It is not not included with Crawlee by default to keep the installation size minimal.
                </p>
                <CodeBlock className="language-bash">
                    pip install 'crawlee[playwright]'
                </CodeBlock>
                <p>
                    Currently we have Python packages <code>crawlee</code> and <code>playwright</code> installed. There is one more essential requirement: the Playwright browser binaries. You can install them by running:
                </p>
                <CodeBlock className="language-bash">
                    playwright install
                </CodeBlock>
                <p>
                    Now we are ready to execute our first Crawlee project:
                </p>
                <CodeBlock className="language-python">
                    {HomePageExample}
                </CodeBlock>
            </div>
        </section>
    );
}

// const npmInstall = `pip install apify
// npm install -g apify-cli`;
// const exampleActor = `import { PlaywrightCrawler, Dataset } from 'crawlee';
//
// // Import the \`Actor\` class from the Apify SDK.
// import { Actor } from 'apify';
//
// // Set up the integration to Apify.
// await Actor.init();
//
// // Crawler setup from the previous example.
// const crawler = new PlaywrightCrawler({
//     // ...
// });
// await crawler.run(['https://crawlee.dev']);
//
// // Once finished, clean up the environment.
// await Actor.exit();
// `;
//
// const apifyPush = `apify login # so the CLI knows you
// apify init  # and the Apify platform understands your project
// apify push  # time to ship it!`;
//
// function Deployment() {
//     return (
//         <section className={clsx(styles.try, 'container')}>
//             <div className="col">
//                 <h2>Deploy to the cloud ☁️</h2>
//                 <p>
//                     Crawlee is developed by <a href="https://apify.com" rel="dofollow noreferrer" target="_blank"><b>Apify</b></a>, the web scraping and automation platform.
//                     You can deploy a <b>Crawlee</b> project wherever you want (see our deployment guides for <a href="https://crawlee.dev/docs/deployment/aws-cheerio"><b>AWS
//                     Lambda</b></a> and <a href="https://crawlee.dev/docs/deployment/gcp-cheerio"><b>Google Cloud</b></a>), but using the&nbsp;
//                     <a href="https://console.apify.com/" target="_blank" rel="noreferrer"><b>Apify platform</b></a> will give you the best experience. With a few simple steps,
//                     you can convert your <b>Crawlee</b> project into a so-called <b>Actor</b>. Actors are serverless micro-apps that are easy to develop, run,
//                     share, and integrate. The infra, proxies, and storages are ready to go. <a href="https://apify.com/actors" target="_blank" rel="noreferrer">Learn more about Actors</a>.
//                 </p>
//                 <p>
//                     1️⃣ First, install the <b>Apify SDK</b> to your project, as well as the <b>Apify CLI</b>. The SDK will help with the Apify integration,
//                     while the CLI will help us with the initialization and deployment.
//                 </p>
//                 <CodeBlock className="language-bash">
//                     {npmInstall}
//                 </CodeBlock>
//                 <p>
//                     2️⃣ The next step is to add <code>Actor.init()</code> to the beginning of your main script and <code>Actor.exit()</code> to the end of it.
//                     This will enable the integration to the Apify platform, so the <a href="https://apify.com/storage" target="_blank" rel="noreferrer">cloud
//                     storages</a> (e.g. <code>RequestQueue</code>) will be used. The code should look like this:
//                 </p>
//                 <RunnableCodeBlock className="language-typescript" type="playwright">
//                     {{
//                         code: exampleActor,
//                         hash: 'eyJ1IjoiRWdQdHczb2VqNlRhRHQ1cW4iLCJ2IjoxfQ.eyJpbnB1dCI6IntcbiAgICBcImNvZGVcIjogXCJpbXBvcnQgeyBQbGF5d3JpZ2h0Q3Jhd2xlciB9IGZyb20gJ2NyYXdsZWUnO1xcblxcbi8vIEltcG9ydCB0aGUgYEFjdG9yYCBjbGFzcyBmcm9tIHRoZSBBcGlmeSBTREsuXFxuaW1wb3J0IHsgQWN0b3IgfSBmcm9tICdhcGlmeSc7XFxuXFxuLy8gU2V0IHVwIHRoZSBpbnRlZ3JhdGlvbiB0byBBcGlmeS5cXG5hd2FpdCBBY3Rvci5pbml0KCk7XFxuXFxuLy8gQ3Jhd2xlciBzZXR1cCBmcm9tIHRoZSBwcmV2aW91cyBleGFtcGxlLlxcbmNvbnN0IGNyYXdsZXIgPSBuZXcgUGxheXdyaWdodENyYXdsZXIoe1xcbiAgICAvLyBVc2UgdGhlIHJlcXVlc3RIYW5kbGVyIHRvIHByb2Nlc3MgZWFjaCBvZiB0aGUgY3Jhd2xlZCBwYWdlcy5cXG4gICAgYXN5bmMgcmVxdWVzdEhhbmRsZXIoeyByZXF1ZXN0LCBwYWdlLCBlbnF1ZXVlTGlua3MsIHB1c2hEYXRhLCBsb2cgfSkge1xcbiAgICAgICAgY29uc3QgdGl0bGUgPSBhd2FpdCBwYWdlLnRpdGxlKCk7XFxuICAgICAgICBsb2cuaW5mbyhgVGl0bGUgb2YgJHtyZXF1ZXN0LmxvYWRlZFVybH0gaXMgJyR7dGl0bGV9J2ApO1xcblxcbiAgICAgICAgLy8gU2F2ZSByZXN1bHRzIGFzIEpTT04gdG8gLi9zdG9yYWdlL2RhdGFzZXRzL2RlZmF1bHRcXG4gICAgICAgIGF3YWl0IHB1c2hEYXRhKHsgdGl0bGUsIHVybDogcmVxdWVzdC5sb2FkZWRVcmwgfSk7XFxuXFxuICAgICAgICAvLyBFeHRyYWN0IGxpbmtzIGZyb20gdGhlIGN1cnJlbnQgcGFnZVxcbiAgICAgICAgLy8gYW5kIGFkZCB0aGVtIHRvIHRoZSBjcmF3bGluZyBxdWV1ZS5cXG4gICAgICAgIGF3YWl0IGVucXVldWVMaW5rcygpO1xcbiAgICB9LFxcblxcbiAgICAvLyBVbmNvbW1lbnQgdGhpcyBvcHRpb24gdG8gc2VlIHRoZSBicm93c2VyIHdpbmRvdy5cXG4gICAgLy8gaGVhZGxlc3M6IGZhbHNlLFxcblxcbiAgICAvLyBVbmNvbW1lbnQgdGhpcyBvcHRpb24gdG8gc2NyYXBlIHRoZSBmdWxsIHdlYnNpdGUuXFxuICAgIG1heFJlcXVlc3RzUGVyQ3Jhd2w6IDIwLFxcbn0pO1xcblxcbi8vIEFkZCBmaXJzdCBVUkwgdG8gdGhlIHF1ZXVlIGFuZCBzdGFydCB0aGUgY3Jhd2wuXFxuYXdhaXQgY3Jhd2xlci5ydW4oWydodHRwczovL2NyYXdsZWUuZGV2J10pO1xcblxcbi8vIEV4cG9ydCB0aGUgZW50aXJldHkgb2YgdGhlIGRhdGFzZXQgdG8gYSBzaW5nbGUgZmlsZSBpblxcbi8vIC4vc3RvcmFnZS9rZXlfdmFsdWVfc3RvcmVzL3Jlc3VsdC5jc3ZcXG5jb25zdCBkYXRhc2V0ID0gYXdhaXQgY3Jhd2xlci5nZXREYXRhc2V0KCk7XFxuYXdhaXQgZGF0YXNldC5leHBvcnRUb0NTVigncmVzdWx0Jyk7XFxuXFxuLy8gT3Igd29yayB3aXRoIHRoZSBkYXRhIGRpcmVjdGx5LlxcbmNvbnN0IGRhdGEgPSBhd2FpdCBjcmF3bGVyLmdldERhdGEoKTtcXG5jb25zb2xlLmxvZyhkYXRhLml0ZW1zLnNsaWNlKDAsIDUpKTtcXG5cXG4vLyBPbmNlIGZpbmlzaGVkLCBjbGVhbiB1cCB0aGUgZW52aXJvbm1lbnQuXFxuYXdhaXQgQWN0b3IuZXhpdCgpO1xcblwiXG59Iiwib3B0aW9ucyI6eyJjb250ZW50VHlwZSI6ImFwcGxpY2F0aW9uL2pzb247IGNoYXJzZXQ9dXRmLTgiLCJtZW1vcnkiOjQwOTZ9fQ.Te7qi0ocWNsH3ujFkgIv8AO9GQ5Wk4DZeQ9-zHTy7Vo',
//                     }}
//                 </RunnableCodeBlock>
//                 <p>
//                     3️⃣ Then you will need to <a href="https://console.apify.com/sign-up" target="_blank" rel="noreferrer">sign up for the Apify account</a>. Once you have it,
//                     use the Apify CLI to log in via <code>apify login</code>. The last two steps also involve the Apify CLI. Call the <code>apify
//                     init</code> first, which will add Apify config to your project, and finally run the <code>apify push</code> to deploy it.
//                 </p>
//                 <CodeBlock className="language-bash">
//                     {apifyPush}
//                 </CodeBlock>
//             </div>
//         </section>
//     );
// }

export default function Home() {
    const SvgLogo = require('../../static/img/crawlee-logo.svg').default;
    const { siteConfig } = useDocusaurusContext();
    return (
        <Layout
            description={siteConfig.description}>
            <Hero />
            <Features />
            <Highlights />
            <ActorExample />
            {/*<Deployment />*/}
            <div className="container">
                <div className="row">
                    <div className="col text--center padding-top--lg padding-bottom--xl">
                        <SvgLogo className={styles.bottomLogo} />
                    </div>
                </div>
            </div>
        </Layout>
    );
}

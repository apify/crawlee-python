import Link from '@docusaurus/Link';
import { useThemeConfig } from '@docusaurus/theme-common';
import useBaseUrl from '@docusaurus/useBaseUrl';
import Logo from '@theme/Logo';
import ThemedImage from '@theme/ThemedImage';
import React from 'react';

import styles from './index.module.css';

export default function LogoWrapper(props) {
    const ArrowsIcon = require('../../../../static/img/menu-arrows.svg').default;
    const CheckIcon = require('../../../../static/img/check.svg').default;
    const { navbar: { logo } } = useThemeConfig();
    const javascriptLogo = {
        light: useBaseUrl('img/crawlee-javascript-light.svg'),
        dark: useBaseUrl('img/crawlee-javascript-dark.svg'),
    };
    const languageAgnosticLogo = {
        light: useBaseUrl('img/crawlee-light.svg'),
        dark: useBaseUrl('img/crawlee-dark.svg'),
    };
    const pythonLogo = {
        light: useBaseUrl(logo.src),
        dark: useBaseUrl(logo.srcDark || logo.src),
    };
    return (
        <div className={styles.navbarLogo}>
            <div className={styles.logoWithArrows}>
                <Logo titleClassName="navbar__title" />
                <ArrowsIcon />
            </div>
            <div className={styles.menuWrapper}>
                <div className={styles.menu}>
                    <Link className={styles.menuItem} href="https://crawlee.dev/js" target="_self" rel="dofollow">
                        <ThemedImage sources={javascriptLogo} alt="Crawlee JavaScript" />
                    </Link>
                    <Link className={styles.menuItem} to="/" >
                        <ThemedImage sources={pythonLogo} alt="Crawlee Python" />
                        <CheckIcon />
                    </Link>
                    <Link className={styles.menuItem} href="https://crawlee.dev" target="_self" rel="dofollow">
                        <ThemedImage sources={languageAgnosticLogo} alt="Crawlee" />
                    </Link>
                </div>
            </div>
        </div >
    );
}

import { useDoc } from '@docusaurus/plugin-content-docs/client';
import { useWindowSize, useColorMode } from '@docusaurus/theme-common';
import Giscus from '@giscus/react';
import DocBreadcrumbs from '@theme/DocBreadcrumbs';
import DocItemContent from '@theme/DocItem/Content';
import DocItemFooter from '@theme/DocItem/Footer';
import DocItemPaginator from '@theme/DocItem/Paginator';
import DocItemTOCDesktop from '@theme/DocItem/TOC/Desktop';
import DocItemTOCMobile from '@theme/DocItem/TOC/Mobile';
import DocVersionBadge from '@theme/DocVersionBadge';
import DocVersionBanner from '@theme/DocVersionBanner';
import clsx from 'clsx';
import React from 'react';

import styles from './styles.module.css';

/**
 * Decide if the toc should be rendered, on mobile or desktop viewports
 */
function useDocTOC() {
    const {
        frontMatter,
        toc,
    } = useDoc();
    const windowSize = useWindowSize();
    const hidden = frontMatter.hide_table_of_contents;
    const canRender = !hidden && toc.length > 0;
    const mobile = canRender ? <DocItemTOCMobile/> : undefined;
    const desktop = canRender && (windowSize === 'desktop' || windowSize === 'ssr') ? (
        <DocItemTOCDesktop/>
    ) : undefined;
    return {
        hidden,
        mobile,
        desktop,
    };
}

export default function DocItemLayout({ children }) {
    const docTOC = useDocTOC();
    const { colorMode } = useColorMode();
    return (
        <div className="row">
            <div className={clsx('col', !docTOC.hidden && styles.docItemCol)}>
                <DocVersionBanner/>
                <div className={styles.docItemContainer}>
                    <article>
                        <DocBreadcrumbs/>
                        <DocVersionBadge/>
                        {docTOC.mobile}
                        <DocItemContent>{children}</DocItemContent>
                        <DocItemFooter/>
                    </article>
                    <DocItemPaginator/>

                    <Giscus
                        id="giscus-comments"
                        repo="apify/crawlee-python"
                        repoId="R_kgDOLDBXgA"
                        category="Comments"
                        categoryId="DIC_kwDOLDBXgM4CgQI1"
                        mapping="pathname"
                        reactionsEnabled="1"
                        emitMetadata="0"
                        inputPosition="top"
                        theme={colorMode}
                        lang="en"
                        strict="1"
                    />
                </div>
            </div>

            {docTOC.desktop && <div className="col col--3">{docTOC.desktop}</div>}
        </div>
    );
}

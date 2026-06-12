import React from 'react';
import Link from '@docusaurus/Link';
import { useDocsVersion } from '@docusaurus/plugin-content-docs/client';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

const ApiLink = ({ to, children }) => {
    const version = useDocsVersion();
    const { siteConfig } = useDocusaurusContext();

    if (siteConfig.presets[0][1].docs.disableVersioning || version.isLast) {
        return (
            <Link to={`/api/${to}`}>{children}</Link>
        );
    }

    return (
        <Link to={`/api/${version.version === 'current' ? 'next' : version.version}/${to}`}>{children}</Link>
    );
};

export default ApiLink;

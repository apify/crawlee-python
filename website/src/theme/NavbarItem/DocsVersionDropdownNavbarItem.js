/**
 * Swizzled DocsVersionDropdownNavbarItem that is aware of API reference pages.
 *
 * The upstream component only handles docs pages. When the user is browsing
 * the API reference (routes under /api), switching versions must navigate to
 * the matching API version path, not to a docs page.
 */
import React from 'react';
import {
    useVersions,
    useActiveDocContext,
    useDocsVersionCandidates,
    useDocsPreferredVersion,
} from '@docusaurus/plugin-content-docs/client';
import { useLocation } from '@docusaurus/router';
import { translate } from '@docusaurus/Translate';
import { useHistorySelector } from '@docusaurus/theme-common';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import DefaultNavbarItem from '@theme/NavbarItem/DefaultNavbarItem';
import DropdownNavbarItem from '@theme/NavbarItem/DropdownNavbarItem';

import { getApiVersionPath } from './apiVersionUtils';

function getVersionItems(versions, configs) {
    if (configs) {
        const versionMap = new Map(versions.map((version) => [version.name, version]));
        const toVersionItem = (name, config) => {
            const version = versionMap.get(name);
            if (!version) {
                throw new Error(
                    `No docs version exist for name '${name}', please verify your 'docsVersionDropdown' navbar item versions config.\nAvailable version names:\n- ${versions.map((v) => `${v.name}`).join('\n- ')}`,
                );
            }
            return { version, label: config?.label ?? version.label };
        };
        if (Array.isArray(configs)) {
            return configs.map((name) => toVersionItem(name, undefined));
        }
        return Object.entries(configs).map(([name, config]) => toVersionItem(name, config));
    }
    return versions.map((version) => ({ version, label: version.label }));
}

function useVersionItems({ docsPluginId, configs }) {
    const versions = useVersions(docsPluginId);
    return getVersionItems(versions, configs);
}

function getVersionMainDoc(version) {
    return version.docs.find((doc) => doc.id === version.mainDocId);
}

function getVersionTargetDoc(version, activeDocContext) {
    return activeDocContext.alternateDocVersions[version.name] ?? getVersionMainDoc(version);
}

function useDisplayedVersionItem({ docsPluginId, versionItems }) {
    const candidates = useDocsVersionCandidates(docsPluginId);
    const candidateItems = candidates
        .map((candidate) => versionItems.find((vi) => vi.version === candidate))
        .filter((vi) => vi !== undefined);
    return candidateItems[0] ?? versionItems[0];
}

/** Detect whether the user is currently on an API reference page and, if so,
 *  determine which version they are viewing. */
function useApiVersionInfo(baseUrl, versions) {
    const { pathname } = useLocation();
    const apiPrefix = `${baseUrl}api`;

    if (!pathname.startsWith(apiPrefix)) {
        return null;
    }

    const afterApi = pathname.slice(apiPrefix.length);
    const segments = afterApi.split('/').filter(Boolean);

    if (segments.length > 0 && segments[0] === 'next') {
        return { currentVersionName: 'current' };
    }

    if (segments.length > 0) {
        const versionNames = new Set(versions.map((v) => v.name));
        if (versionNames.has(segments[0])) {
            return { currentVersionName: segments[0] };
        }
    }

    const lastVersion = versions.find((v) => v.isLast);
    return { currentVersionName: lastVersion?.name };
}

export default function DocsVersionDropdownNavbarItem({
    mobile,
    docsPluginId,
    dropdownActiveClassDisabled,
    dropdownItemsBefore,
    dropdownItemsAfter,
    versions: configs,
    ...props
}) {
    const { siteConfig } = useDocusaurusContext();
    const { baseUrl } = siteConfig;
    const search = useHistorySelector((history) => history.location.search);
    const hash = useHistorySelector((history) => history.location.hash);
    const activeDocContext = useActiveDocContext(docsPluginId);
    const { savePreferredVersionName } = useDocsPreferredVersion(docsPluginId);
    const versionItems = useVersionItems({ docsPluginId, configs });
    const displayedVersionItem = useDisplayedVersionItem({ docsPluginId, versionItems });

    const versions = useVersions(docsPluginId);
    const apiInfo = useApiVersionInfo(baseUrl, versions);
    const isOnApiPage = apiInfo !== null;

    function versionItemToLink({ version, label }) {
        if (isOnApiPage) {
            const apiPath = getApiVersionPath(version, baseUrl);
            return {
                label,
                to: `${apiPath}${search}${hash}`,
                isActive: () => version.name === apiInfo.currentVersionName,
                onClick: () => savePreferredVersionName(version.name),
            };
        }

        const targetDoc = getVersionTargetDoc(version, activeDocContext);
        return {
            label,
            to: `${targetDoc.path}${search}${hash}`,
            isActive: () => version === activeDocContext.activeVersion,
            onClick: () => savePreferredVersionName(version.name),
        };
    }

    // When on an API page, show the version matching the current API path.
    let effectiveDisplayedItem = displayedVersionItem;
    if (isOnApiPage) {
        const match = versionItems.find((vi) => vi.version.name === apiInfo.currentVersionName);
        if (match) {
            effectiveDisplayedItem = match;
        }
    }

    const items = [...dropdownItemsBefore, ...versionItems.map(versionItemToLink), ...dropdownItemsAfter];

    const dropdownLabel =
        mobile && items.length > 1
            ? translate({
                  id: 'theme.navbar.mobileVersionsDropdown.label',
                  message: 'Versions',
                  description: 'The label for the navbar versions dropdown on mobile view',
              })
            : effectiveDisplayedItem.label;

    let dropdownTo;
    if (mobile && items.length > 1) {
        dropdownTo = undefined;
    } else if (isOnApiPage) {
        dropdownTo = getApiVersionPath(effectiveDisplayedItem.version, baseUrl);
    } else {
        dropdownTo = getVersionTargetDoc(effectiveDisplayedItem.version, activeDocContext).path;
    }

    if (items.length <= 1) {
        return (
            <DefaultNavbarItem
                {...props}
                mobile={mobile}
                label={dropdownLabel}
                to={dropdownTo}
                isActive={dropdownActiveClassDisabled ? () => false : undefined}
            />
        );
    }

    return (
        <DropdownNavbarItem
            {...props}
            mobile={mobile}
            label={dropdownLabel}
            to={dropdownTo}
            items={items}
            isActive={dropdownActiveClassDisabled ? () => false : undefined}
        />
    );
}

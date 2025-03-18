import Link from '@docusaurus/Link';
import { useLocation } from '@docusaurus/router';
import { useThemeConfig } from '@docusaurus/theme-common';
import {
    splitNavbarItems,
    useNavbarMobileSidebar,
} from '@docusaurus/theme-common/internal';
import NavbarLogo from '@theme/Navbar/Logo';
import NavbarMobileSidebarToggle from '@theme/Navbar/MobileSidebar/Toggle';
import NavbarSearch from '@theme/Navbar/Search';
import NavbarItem from '@theme/NavbarItem';
import SearchBar from '@theme/SearchBar';
import clsx from 'clsx';
import React from 'react';

import styles from './styles.module.css';

function useNavbarItems() {
    return useThemeConfig().navbar.items;
}

function NavbarItems({ items, className }) {
    return (
        <div className={clsx(styles.navbarItems, className)}>
            {items.map((item, i) => (
                <NavbarItem {...item} key={i} />
            ))}
        </div>
    );
}

function NavbarContentLayout({ left, right }) {
    return (
        <div className="navbar__inner">
            <div className="navbar__items">{left}</div>
            <div className="navbar__items navbar__items--right">{right}</div>
        </div>
    );
}

const VERSIONS_ITEM = {
    type: 'docsVersionDropdown',
    position: 'left',
    label: 'Versions',
    dropdownItemsAfter: [
        {
            href: 'https://sdk.apify.com/docs/guides/getting-started',
            label: '2.2',
        },
        {
            href: 'https://sdk.apify.com/docs/1.3.1/guides/getting-started',
            label: '1.3',
        },
    ],
    dropdownItemsBefore: [],
};

export default function NavbarContent() {
    const location = useLocation();
    const mobileSidebar = useNavbarMobileSidebar();
    const items = useNavbarItems();
    const effectiveItems = location.pathname?.endsWith('/python/')
        || location.pathname?.endsWith('/python')
        ? items
        : [...items, VERSIONS_ITEM];
    const [leftItems, rightItems] = splitNavbarItems(effectiveItems);
    const searchBarItem = items.find((item) => item.type === 'search');
    return (
        <NavbarContentLayout
            left={
                <>
                    <NavbarLogo />
                    <NavbarItems items={leftItems} />
                </>
            }
            right={
                <>
                    {rightItems?.length > 0 && (
                        <NavbarItems items={rightItems} />
                    )}
                    {!searchBarItem && (
                        <NavbarSearch>
                            <SearchBar />
                        </NavbarSearch>
                    )}
                    <Link
                        className={styles.getStartedButton}
                        to="/docs/quick-start"
                    >
                        Get started
                    </Link>
                    {!mobileSidebar.disabled && <NavbarMobileSidebarToggle />}
                </>
            }
        />
    );
}

import { useThemeConfig } from '@docusaurus/theme-common';
import { useNavbarMobileSidebar } from '@docusaurus/theme-common/internal';
import NavbarItem from '@theme/NavbarItem';
import React from 'react';

function useNavbarItems() {
    return useThemeConfig().navbar.items;
}
// The primary menu displays the navbar items
export default function NavbarMobilePrimaryMenu() {
    const mobileSidebar = useNavbarMobileSidebar();
    const items = useNavbarItems();

    return (
        <ul className="menu__list">
            {items.map((item, i) => (
                <NavbarItem
                    mobile
                    {...item}
                    onClick={() => mobileSidebar.toggle()}
                    key={i}
                />
            ))}
        </ul>
    );
}

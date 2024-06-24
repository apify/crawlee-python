import React from 'react';
import clsx from 'clsx';
import {useThemeConfig} from '@docusaurus/theme-common';
import Logo from '@theme/Logo';
import CollapseButton from '@theme/DocSidebar/Desktop/CollapseButton';
import Content from '@theme/DocSidebar/Desktop/Content';
import styles from './styles.module.css';
function DocSidebarDesktop({path, sidebar, onCollapse, isHidden}) {
  const {
    navbar: {hideOnScroll},
    docs: {
      sidebar: {hideable},
    },
  } = useThemeConfig();
  return (
      <div
          className={clsx(
              styles.sidebar,
              hideOnScroll && styles.sidebarWithHideableNavbar,
              isHidden && styles.sidebarHidden,
          )}>
          {hideOnScroll
              && <><Logo tabIndex={-1} className={styles.sidebarLogo}/><span className="navbar__logo_appendix navbar__logo_appendix_sidebar">for Python</span></>}
          <Content path={path} sidebar={sidebar}/>
          {hideable && <CollapseButton onClick={onCollapse}/>}
      </div>
  );
}

export default React.memo(DocSidebarDesktop);

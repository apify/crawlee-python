import { visit } from 'unist-util-visit';
import { URL } from 'url';

const internalUrls = ['crawlee.dev'];

/**
 * Check if the URL is internal.
 * 
 * @param {URL} href - The parsed URL object.
 * @returns {boolean} - Returns true if the URL is internal.
 */
function isInternal(href) {
    return internalUrls.some(
        (internalUrl) => href.hostname === internalUrl
            || (!href.protocol && !href.hostname && (href.pathname || href.hash))
    );
}

/**
 * A unified plugin that processes external links.
 * Adds `target="_blank"` and `rel="noopener"` for external links.
 * 
 * @type {import('unified').Plugin}
 */
export const externalLinkProcessor = () => {
    return (tree) => {
        visit(tree, 'element', (node) => {
            if (
                node.tagName === 'a'
                && node.properties
                && typeof node.properties.href === 'string'
            ) {
                try {
                    const href = new URL(node.properties.href, 'https://example.com'); // Base URL for relative links

                    if (!isInternal(href)) {
                        node.properties.target = '_blank';
                        node.properties.rel = 'noopener';
                    } else {
                        node.properties.target = null;
                        node.properties.rel = null;
                    }
                } catch (error) {
                    console.error(`Error parsing URL: ${node.properties.href}`, error);
                }
            }
        });
    };
};

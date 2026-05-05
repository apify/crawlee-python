const API_ROUTE_BASE = 'api';

/** Build the API path for a given version (relative, no baseUrl). */
export function getApiPath(version) {
    if (version.isLast) {
        return API_ROUTE_BASE;
    }
    if (version.name === 'current') {
        return `${API_ROUTE_BASE}/next`;
    }
    return `${API_ROUTE_BASE}/${version.name}`;
}

/** Build the full API path for a given version (with baseUrl prefix). */
export function getApiVersionPath(version, baseUrl) {
    return `${baseUrl}${getApiPath(version)}`;
}

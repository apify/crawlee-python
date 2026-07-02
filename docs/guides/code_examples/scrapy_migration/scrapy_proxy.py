# settings.py
# Rotation needs the third-party `scrapy-rotating-proxies` package. The built-in
# `HttpProxyMiddleware` reads a single proxy from `request.meta` or the environment.
ROTATING_PROXY_LIST = [
    'http://proxy-1.com/',
    'http://proxy-2.com/',
]

DOWNLOADER_MIDDLEWARES = {
    'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
    'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
}

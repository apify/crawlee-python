from yarl import URL

from crawlee._utils.sitemap import Sitemap


async def test_sitemap(server_url: URL) -> None:
    sitemap_body = """
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
<url>
<loc>http://not-exists.com/</loc>
<lastmod>2005-02-03</lastmod>
<changefreq>monthly</changefreq>
<priority>0.8</priority>
</url>
<url>
<loc>http://not-exists.com/catalog?item=12&amp;desc=vacation_hawaii</loc>
<changefreq>weekly</changefreq>
</url>
<url>
<loc>http://not-exists.com/catalog?item=73&amp;desc=vacation_new_zealand</loc>
<lastmod>2004-12-23</lastmod>
<changefreq>weekly</changefreq>
</url>
<url>
<loc>http://not-exists.com/catalog?item=74&amp;desc=vacation_newfoundland</loc>
<lastmod>2004-12-23T18:00:15+00:00</lastmod>
<priority>0.3</priority>
</url>
<url>
<loc>http://not-exists.com/catalog?item=83&amp;desc=vacation_usa</loc>
<lastmod>2004-11-23</lastmod>
</url>
</urlset>
""".strip()
    sitemap_url = (server_url / 'get_sitemap').with_query(content=sitemap_body)
    sitemap = await Sitemap.load(str(sitemap_url))
    print(len(sitemap.urls))

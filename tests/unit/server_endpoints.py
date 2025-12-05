# Test server response content for testing

HELLO_WORLD = b"""\
<html><head>
    <title>Hello, world!</title>
</head>
<body>
</body></html>"""

START_ENQUEUE = b"""\
<html><head>
    <title>Hello</title>
</head>
<body>
    <a href="/sub_index" class="foo">Link 1</a>
    <a href="/page_1">Link 2</a>
    <a href="mailto:test@test.com">test@test.com</a>
</body></html>"""

SECONDARY_INDEX = b"""\
<html><head>
    <title>Hello</title>
</head>
<body>
    <a href="/page_3">Link 3</a>
    <a href="/page_2">Link 4</a>
    <a href="/base_page">Base Page</a>
</body></html>"""

BASE_INDEX = """\
<html><head>
    <base href="{host}/base_subpath/">
    <base href="{host}/sub_index/">
    <title>Hello</title>
</head>
<body>
    <a href="page_5">Link 5</a>
    <a href="/page_4">Link 6</a>
</body></html>"""

INCAPSULA = b"""\
<html><head>
    <title>Hello</title>
</head>
<body>
    <iframe src=Test_Incapsula_Resource>
    </iframe>
</body></html>"""

PROBLEMATIC_LINKS = b"""\
<html><head>
    <title>Hello</title>
</head>
<body>
    <a href="https://budplaceholder.com/">Placeholder</a>
    <a href="mailto:test@test.com">test@test.com</a>
    <a href=https://avatars.githubusercontent.com/apify>Apify avatar/a>
</body></html>"""

GENERIC_RESPONSE = b"""\
<html><head>
    <title>Hello</title>
</head>
<body>
    Insightful content
</body></html>"""


ROBOTS_TXT = b"""\
User-agent: *
Disallow: *deny_all/
Disallow: /page_
crawl-delay: 10

User-agent: Googlebot
Disallow: *deny_googlebot/
crawl-delay: 1

user-agent: Mozilla
crawl-delay: 2

sitemap: http://not-exists.com/sitemap_1.xml
sitemap: http://not-exists.com/sitemap_2.xml"""


INFINITE_SCROLL = b"""\
<!DOCTYPE html>
<html>
<body>
    <div id="content"></div>

    <script>
        let page = 0;
        let loading = false;

        for (let i = 0; i < 10; i++) {
            const div = document.createElement('div');
            div.className = 'item';
            div.style.height = '200px';
            div.textContent = 'Item ' + (i + 1);
            document.getElementById('content').appendChild(div);
        }

        async function loadMore() {
            if (loading || page >= 3) return;
            loading = true;
            page++;

            await new Promise(resolve => setTimeout(resolve, 100));

            for (let i = 0; i < 10; i++) {
                const div = document.createElement('div');
                div.className = 'item';
                div.style.height = '200px';
                div.textContent = 'Item ' + (page * 10 + i + 1);
                document.getElementById('content').appendChild(div);
            }

            loading = false;
        }

        window.addEventListener('scroll', () => {
            if (window.innerHeight + window.scrollY >= document.body.offsetHeight - 100) {
                loadMore();
            }
        });
    </script>
</body>
</html>
"""

RESOURCE_LOADING_PAGE = b"""\
<!DOCTYPE html>
<html>
  <head>
    <script src="/server_static/test.js"></script>
  </head>
  <body>
    <img src="/server_static/test.png" />
  </body>
</html>
"""

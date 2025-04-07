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
</body></html>"""

SECONDARY_INDEX = b"""\
<html><head>
    <title>Hello</title>
</head>
<body>
    <a href="/page_3">Link 3</a>
    <a href="/page_2">Link 4</a>
</body></html>"""

INCAPSULA = b"""\
<html><head>
    <title>Hello</title>
</head>
<body>
    <iframe src=Test_Incapsula_Resource>
    </iframe>
</body></html>"""

GENERIC_RESPONSE = b"""\
<html><head>
    <title>Hello</title>
</head>
<body>
    Insightful content
</body></html>"""

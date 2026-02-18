# % if cookiecutter.crawler_type.startswith('playwright')
# % include 'routes_playwright.py'
# % else
# % include 'routes_%s.py' % cookiecutter.__crawler_type
# % endif

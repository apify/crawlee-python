"""
Replaces the default pydoc-markdown shell script with a custom Python script calling the pydoc-markdown API directly.

This script generates an AST from the Python source code in the `src` directory and prints it as a JSON object.
"""

from pydoc_markdown.interfaces import Context
from pydoc_markdown.contrib.loaders.python import PythonLoader
from pydoc_markdown.contrib.processors.filter import FilterProcessor
from pydoc_markdown.contrib.processors.crossref import CrossrefProcessor
from pydoc_markdown.contrib.renderers.markdown import MarkdownReferenceResolver
from google_docstring_processor import ApifyGoogleProcessor
from docspec import dump_module

import json
import os

project_path = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../src'))

context = Context(directory='.')
loader = PythonLoader(search_path=[project_path])
filter = FilterProcessor(
    documented_only=False,
    skip_empty_modules=False,
)
crossref = CrossrefProcessor()
google = ApifyGoogleProcessor()

loader.init(context)
filter.init(context)
google.init(context)
crossref.init(context)

processors = [filter, google, crossref]

dump = []

modules = list(loader.load())

for processor in processors:
    processor.process(modules, None)

for module in modules:
    dump.append(dump_module(module))

repo_root_path = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../'))

print(
    json.dumps(dump, indent=4).replace(
        repo_root_path,
        'REPO_ROOT_PLACEHOLDER'
    )
)

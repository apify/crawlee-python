#!/bin/bash

# Create docspec dump of this package's source code through pydoc-markdown
python ./pydoc-markdown/generate_ast.py > docspec-dump.jsonl

rm -rf "${apify_shared_tempdir}"

# Generate import shortcuts from the modules
python generate_module_shortcuts.py

# Transform the docpec dumps into Typedoc-compatible docs tree
node transformDocs.js

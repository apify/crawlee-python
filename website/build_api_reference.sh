#!/bin/bash

# On macOS, sed requires a space between -i and '' to specify no backup should be done
# On Linux, sed requires no space between -i and '' to specify no backup should be done
sed_no_backup() {
    if [[ $(uname) = "Darwin" ]]; then
        sed -i '' "$@"
    else
        sed -i'' "$@"
    fi
}

# Create docspec dump of this package's source code through pydoc-markdown
python ./pydoc-markdown/generate_ast.py > docspec-dump.jsonl
sed_no_backup "s#${PWD}/..#REPO_ROOT_PLACEHOLDER#g" docspec-dump.jsonl

rm -rf "${apify_shared_tempdir}"

# Generate import shortcuts from the modules
python generate_module_shortcuts.py

# Transform the docpec dumps into Typedoc-compatible docs tree
node transformDocs.js

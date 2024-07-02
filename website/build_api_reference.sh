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
pydoc-markdown --quiet --dump > docspec-dump.jsonl
sed_no_backup "s#${PWD}/..#REPO_ROOT_PLACEHOLDER#g" docspec-dump.jsonl

# Create docpec dump from the right version of the apify-shared package
apify_shared_version=$(python -c "import apify_shared; print(apify_shared.__version__)")
apify_shared_tempdir=$(realpath "$(mktemp -d)")
git clone --quiet https://github.com/apify/apify-shared-python.git "${apify_shared_tempdir}"
cp ./pydoc-markdown.yml "${apify_shared_tempdir}/pydoc-markdown.yml"
sed_no_backup "s#search_path: \[../src\]#search_path: \[./src\]#g" "${apify_shared_tempdir}/pydoc-markdown.yml"

(
    cd "${apify_shared_tempdir}";
    git checkout --quiet "v${apify_shared_version}";
    pydoc-markdown --quiet --dump > ./apify-shared-docspec-dump.jsonl
)

cp "${apify_shared_tempdir}/apify-shared-docspec-dump.jsonl" .
sed_no_backup "s#${apify_shared_tempdir}#REPO_ROOT_PLACEHOLDER#g" apify-shared-docspec-dump.jsonl

rm -rf "${apify_shared_tempdir}"

# Generate import shortcuts from the modules
python generate_module_shortcuts.py

# Transform the docpec dumps into Typedoc-compatible docs tree
node transformDocs.js

#!/bin/bash
# Returns true if only the docs have been modified

set -x

DOCS_REPO="docs/"

FILES_CHANGED=$(git diff --name-only origin/develop...HEAD)

if [[ -z $FILES_CHANGED ]]; then
    # no files have changed, likely a merge commit into develop
    echo "No file changes detected, not a docs only commit" >&2
    exit 2;
fi

for file in $FILES_CHANGED
do
    if [[ $file != ${DOCS_REPO}* ]]; then
        echo "No docs changes detected in $file" >&2
        exit 1;
    fi
done

cat <<EOF
======================================================
We detected only docs changes, take appropriate action
======================================================
EOF

exit 0

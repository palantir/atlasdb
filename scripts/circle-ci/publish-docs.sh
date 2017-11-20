#!/usr/bin/env bash

# TODO(ssouza): merge this file and publish-github-page.sh

set -x -e

[[ $CIRCLE_NODE_INDEX == 0 ]] && curl -s --fail $DOCS_URL | bash -s -- -r requirements.txt $CIRCLE_BRANCH


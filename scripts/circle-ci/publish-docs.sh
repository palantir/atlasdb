#!/usr/bin/env bash

set -x -e

[[ $CIRCLE_NODE_INDEX == 0 ]] && curl -s --fail $DOCS_URL | bash -s -- -r requirements.txt $CIRCLE_BRANCH


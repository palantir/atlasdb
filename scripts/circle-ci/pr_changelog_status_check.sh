#!/bin/bash

set -x

CHANGELOG="docs/source/release_notes/release-notes.rst"

if [ -z "$GITHUB_AUTH_TOKEN" ];
then
    exit 1
fi

cd $(dirname $0)

CURRENT_REF=$(git log -1 --format="%H")
CHANGELOG_MODIFIED=$(git diff origin/develop...HEAD --name-only | grep $CHANGELOG -q >/dev/null)$?

if [ $CHANGELOG_MODIFIED -ne 0 ];
then
    curl -X POST -f --silent --header "Authorization: token $GITHUB_AUTH_TOKEN" -d '{"state": "failure", "description": "No modifications to release_notes.rst were found.", "context" : "changelog"}' "https://api.github.com/repos/palantir/atlasdb/statuses/$CURRENT_REF" >/dev/null
else
    curl -X POST -f --silent --header "Authorization: token $GITHUB_AUTH_TOKEN" -d '{"state": "success", "description": "release_notes.rst updated.", "context": "changelog"}' "https://api.github.com/repos/palantir/atlasdb/statuses/$CURRENT_REF" >/dev/null
fi

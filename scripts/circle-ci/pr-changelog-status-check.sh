#!/bin/bash

set -x

CHANGELOG="docs/source/release_notes/release-notes.rst"

if [ -z "$GITHUB_AUTH_TOKEN" ];
then
    exit 1
fi

cd $(dirname $0)

CURRENT_REF=$(git log -1 --format="%H")
CHANGELOG_COMMIT_FLAG=$(git log --pretty=format:%B origin/develop..HEAD | grep -iq '\[no release notes\]')$?
CHANGELOG_MODIFIED=$(git log --name-only --pretty=format: origin/develop..HEAD | grep -q $CHANGELOG)$?

if [ $CHANGELOG_COMMIT_FLAG -eq 0 ]; then
    curl -X POST -f --silent --header "Authorization: token $GITHUB_AUTH_TOKEN" -d '{"state": "success", "description": "Bypassed with commit flag", "context": "changelog"}' "https://api.github.com/repos/palantir/atlasdb/statuses/$CURRENT_REF" >/dev/null
elif [ $CHANGELOG_MODIFIED -eq 0 ]; then
    curl -X POST -f --silent --header "Authorization: token $GITHUB_AUTH_TOKEN" -d '{"state": "success", "description": "release_notes.rst updated.", "context": "changelog"}' "https://api.github.com/repos/palantir/atlasdb/statuses/$CURRENT_REF" >/dev/null
elif ./scripts/circle-ci/check-only-docs-changes.sh; then
    curl -X POST -f --silent --header "Authorization: token $GITHUB_AUTH_TOKEN" -d '{"state": "success", "description": "Only docs updated!", "context": "changelog"}' "https://api.github.com/repos/palantir/atlasdb/statuses/$CURRENT_REF" >/dev/null
else
    curl -X POST -f --silent --header "Authorization: token $GITHUB_AUTH_TOKEN" -d '{"state": "failure", "description": "No modifications found in release_notes.rst, bypass by adding [no release notes] to a commit in this PR", "context" : "changelog"}' "https://api.github.com/repos/palantir/atlasdb/statuses/$CURRENT_REF" >/dev/null
fi

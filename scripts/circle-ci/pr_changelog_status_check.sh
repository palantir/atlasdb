#!/bin/bash

set -x

CHANGELOG="docs/source/release_notes/release-notes.rst"

if [ -z "$GITHUB_AUTH_TOKEN" ];
then
    exit 1
fi

cd $(dirname $0)

CURRENT_REF=$(git log -1 --format="%H")
CHANGELOG_COMMIT_FLAG=$(git log origin/develop..HEAD --pretty=format:%B | grep '\[no release notes\]' -q >/dev/null)$?

if [ $CHANGELOG_COMMIT_FLAG -eq 0];
then
    curl -X POST -f --silent --header "Authorization: token $GITHUB_AUTH_TOKEN" -d '{"state": "success", "description": "by passed with commit flag", "context": "changelog"}' "https://api.github.com/repos/palantir/atlasdb/statuses/$CURRENT_REF" >/dev/null
else
    CHANGELOG_MODIFIED=$(git log origin/develop..HEAD --name-only --pretty=format: | grep $CHANGELOG -q >/dev/null)$?
    if [ $CHANGELOG_MODIFIED -ne 0 ];
    then
        curl -X POST -f --silent --header "Authorization: token $GITHUB_AUTH_TOKEN" -d '{"state": "failure", "description": "No modifications found in release_notes.rst, bypass by adding [no release notes] to a commit in this PR", "context" : "changelog"}' "https://api.github.com/repos/palantir/atlasdb/statuses/$CURRENT_REF" >/dev/null
    else
        curl -X POST -f --silent --header "Authorization: token $GITHUB_AUTH_TOKEN" -d '{"state": "success", "description": "release_notes.rst updated.", "context": "changelog"}' "https://api.github.com/repos/palantir/atlasdb/statuses/$CURRENT_REF" >/dev/null
    fi
fi

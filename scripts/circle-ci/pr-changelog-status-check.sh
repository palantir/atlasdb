#!/bin/bash

set -x

CHANGELOG="docs/source/release_notes/release-notes.rst"

cd $(dirname $0)

CURRENT_REF=$(git log -1 --format="%H")
CHANGELOG_COMMIT_FLAG=$(git log --pretty=format:%B origin/develop..HEAD | grep -iq '\[no release notes\]')$?
CHANGELOG_MODIFIED=$(git log --name-only --pretty=format: origin/develop..HEAD | grep -q $CHANGELOG)$?
CHANGED_FILES=$(git log --name-only --pretty=format: origin/develop..HEAD)

success() {
    message=$1
    if [ -z "$GITHUB_AUTH_TOKEN" ]; then
        echo "$message"
    else
        curl -X POST -f --silent --header "Authorization: token $GITHUB_AUTH_TOKEN" -d '{"state": "success", "description": "'"$message"'", "context": "changelog"}' "https://api.github.com/repos/palantir/atlasdb/statuses/$CURRENT_REF" >/dev/null
    fi
    exit 0
}

fail() {
    message=$1
    if [ -z "$GITHUB_AUTH_TOKEN" ]; then
        echo "$message"
        exit 1
    else
        curl -X POST -f --silent --header "Authorization: token $GITHUB_AUTH_TOKEN" -d '{"state": "failure", "description": "'"$message"'", "context": "changelog"}' "https://api.github.com/repos/palantir/atlasdb/statuses/$CURRENT_REF" >/dev/null
        exit 0
    fi
}

if [ -z "$CHANGED_FILES" ]; then
    success "No files have been changed"
elif [ $CHANGELOG_COMMIT_FLAG -eq 0 ]; then
    success "Bypassed with commit flag"
elif [ $CHANGELOG_MODIFIED -eq 0 ]; then
    success "release_notes.rst updated"
elif ./check-only-docs-changes.sh; then
    success "Only docs updated"
else
    fail "No modifications found in release_notes.rst, bypass by adding [no release notes] to a commit in this PR"
fi

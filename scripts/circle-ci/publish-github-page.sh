#!/usr/bin/env bash

set -x -e

# Just-in-time install of sphinx so that docs build
sudo -H pip install --disable-pip-version-check --upgrade sphinx sphinx_rtd_theme requests recommonmark

# Clone gh-pages into build
cd docs/
rm -rf build/
mkdir ~/.ssh/ && touch ~/.ssh/known_hosts
ssh-keyscan -H github.com >> ~/.ssh/known_hosts
git clone git@github.com:palantir/atlasdb.git -b gh-pages build

# Rebuild the docs into the repo
make html || { echo "doc build failed, build should not pass"; exit 1; }

cd build/
# Just to report status of repo in the build output
git status
git config user.email "pd-atlasdb-team@palantir.com"
git config user.name $CIRCLE_PROJECT_USERNAME

# Add and commmit changes to gh-pages
git add .

if ! git commit -m "circle-publish: [skip ci] from $CIRCLE_BRANCH by build $CIRCLE_BUILD_NUM, can be found at $CIRCLE_BUILD_URL"; then
    echo "no docs changes, skipping deployment";
    exit 0;
fi

git push origin gh-pages:gh-pages

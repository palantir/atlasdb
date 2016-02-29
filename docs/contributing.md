---
title: Contributing
---

## Contributing to AtlasDB

Start by getting a [local development
setup](/docs/getting_started.html#running-from-source) working.  We are happy
to accept pull requests and could use help implementing the AtlasDB API for
more underlying physical stores.

### Release Schedule

We aim to release once a week, but may release faster to patch bugs or slower
if there is low code churn.  We are still setting up a formal branching model,
but for now the `develop` branch serves as `develop`, `release`, and `master`.
This should change soon.

### Cutting for Release

1. Checkout the commit to tag for release (probably `git checkout origin/develop`).
2. Tag the commit: `git tag -a <version>`.
3. Push the tag to Github: `git push origin <version>:refs/tags/<version>`.
4. After a short wait you can publish the artifacts on Bintray by going to the
[AtlasDB page](https://bintray.com/palantir/releases/atlasdb/view). **Note: You
must have admin privledges on Bintray for this to work.**

## Contributing Docs

1. Clone the git repository: `git clone git@github.com:palantir/atlasdb.git; cd
atlasdb`.
2. Checkout the github-pages branch: `git checkout gh-pages`.
3. Edit existing pages (found in `docs/`) using markdown. Create new pages by
adding a markdown file to `docs/` and then adding the appropriate navigation
configuration in `_data/atlasdb/sidebar.yml`.

The docs are a fork of the [jekyll documentation
theme](https://github.com/tomjohnson1492/documentation-theme-jekyll).  See [the
theme
documentation](http://idratherbewriting.com/documentation-theme-jekyll/mydoc/home.html)
for more help.

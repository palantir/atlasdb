===============
Release Process
===============

Release Notes
================

When you make a PR to Atlas, a release notes change must be made to record your change or an explicit reason given why it is unnecessary.
Unless the `no changelog` tag is present, the `changelog-bot` will leave an interactive comment on your PR. Please select the appropriate change type as listed in the comment.

Release Schedule
================

We aim to release once a week, but may release faster to patch bugs or slower if there is low code churn.
If you'd like a release candidate published for testing, please leave a comment on your PR.

Cutting for Release
===================

From a PR, add the `autorelease` tag. When your PR is merged, the `autorelease` bot will automatically trigger a release based on all change types since the last version.
If the PR has already merged and you are an AtlasDB maintainer, head to `the autorelease page <https://autorelease.general.dmz.palantir.tech/palantir/atlasdb>`__ and trigger a release.

After a short wait (when the build at circleCI is successful) the artifacts will be published to Maven Central Repository.
View them at `the Maven Central AtlasDB page <https://search.maven.org/search?q=g:com.palantir.atlasdb>`__.

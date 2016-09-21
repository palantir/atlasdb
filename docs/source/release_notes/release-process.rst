===============
Release Process
===============

Release Notes
================

When you make a PR to Atlas, a release notes change must be made to record your change or an explicit reason given why it is unnecessary. At the same time, you should consider whether your change is breaking or not and update the version number appropriately based on the previous release number.

Release Schedule
================

We aim to release once a week, but may release faster to patch bugs or slower if there is low code churn.
We also publish snapshot builds to `JFrog <https://oss.jfrog.org/webapp/#/artifacts/browse/simple/General/oss-snapshot-local/com/palantir/atlasdb/atlasdb-api>`__ after every PR merges, so if you want to begin testing your changes immediately, you can do so with the snapshot builds rather than waiting until the next release.

Cutting for Release
===================

1. Checkout the commit to tag for release (probably ``git checkout origin/develop``). 
   Patch releases should not be cut from develop, and should instead be cut from an appropriate release branch. 
2. Tag the commit: ``git tag -a <version>``.
3. Push the tag to Github:
   ``git push origin <version>:refs/tags/<version>``.
4. After a short wait (when the build at circleCI is successful) the artifacts will be published on BinTray
   to the `AtlasDB
   page <https://bintray.com/palantir/releases/atlasdb/view>`__.
5. Edit the release notes that will appear for editing on AtlasDB GitHub `release
   page <https://github.com/palantir/atlasdb/releases>`__.

=======================
Contributing to AtlasDB
=======================

Start by getting a `local development
setup <../overview/getting_started.html#running-from-source>`__
working. We are happy to accept pull requests and could use help
implementing the AtlasDB API for more underlying physical stores.

Release Schedule
----------------

We aim to release once a week, but may release faster to patch bugs or
slower if there is low code churn. We are still setting up a formal
branching model, but for now the ``develop`` branch serves as
``develop`` and ``release`` (we have no ``release`` branch) and we merge
releases into ``master``. This should change soon.

Cutting for Release
-------------------

1. Checkout the commit to tag for release (probably
   ``git checkout origin/develop``).
2. Tag the commit: ``git tag -a <version>``.
3. Push the tag to Github:
   ``git push origin <version>:refs/tags/<version>``.
4. After a short wait you can publish the artifacts on Bintray by going
   to the `AtlasDB
   page <https://bintray.com/palantir/releases/atlasdb/view>`__. **Note:
   You must have admin privledges on Bintray for this to work.**

Contributing Docs
=================

1. Clone the git repository:
   ``git clone git@github.com:palantir/atlasdb.git; cd atlasdb``.
2. Checkout the github-pages branch: ``git checkout gh-pages``.
3. Install sphinx if you don't have it.  Instructions can be found `here <http://www.sphinx-doc.org/en/stable/install.html>`__.  The easiest option for Mac users will be using pip.
4. Confirm you installtion is working by running ``cd docs && make html``
5. Existing pages can be found in ``docs/source``. Create new pages by adding an .rst file to ``docs/source`` and then adding the file name to the appropriate .toctree in the parent file.
6. Prior to checking in changes be sure to generate and check in the associated html files by running ``make html`` and ``git add build/html/``.

You can generate and view the docs locally by running
``make html`` and opening the built html files in ``docs/build/html/`` in your browser of choice.

The docs are built using `Sphinx <http://www.sphinx-doc.org/en/stable/index.html>`__ and written in `.rst <http://docutils.sourceforge.net/rst.html>`__.

=======================
Contributing to AtlasDB
=======================

Follow the steps provided in :ref:`running-from-source`.
We are happy to accept pull requests and could use help
implementing the AtlasDB API for more underlying physical stores.

Contributing Docs
=================

The easiest way to contribute to docs is to edit them directly on GitHub using the link above:

1. On GitHub you will see the .rst file rendered and can edit it directly in your browser and preview the changes!
2. After making your edits you can submit a PR below the editor.  Please construct a simple commit message highlighting your changes and create a branch name preferably with the pattern ``docs/*``.

If you would like to build the docs locally follow these steps:

1. Clone the git repository:``git clone git@github.com:palantir/atlasdb.git; cd atlasdb``.
2. Install sphinx and sphinx_rtd_theme if you don't have it.  Instructions can be found `here <http://www.sphinx-doc.org/en/stab/install.html>`__.  The easiest option for Mac users will be using pip.  Please be sure to update sphinx if you haven't used it in a while.  Also please be aware the exact commands to build the docs can be found in ``scripts/circle-ci/publish-github-pages.sh``.
3. Confirm you installtion is working by running ``cd docs && make html``.  The freshly built documentation will be in ``docs/build/html``.
4. Existing pages can be found in ``docs/source``. Create new pages by adding an .rst file to ``docs/source`` and then adding the file name to the appropriate .toctree in the parent file.
5. Commit any changes to a ``docs/*`` branch and push them up to github for review.

You can generate and view the docs locally by running
``make html`` and opening the built html files in ``docs/build/html/`` in your browser of choice.

The docs are built using `Sphinx <http://www.sphinx-doc.org/en/stable/index.html>`__ and written in `.rst <http://docutils.sourceforge.net/rst.html>`__.

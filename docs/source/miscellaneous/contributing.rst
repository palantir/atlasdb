=======================
Contributing to AtlasDB
=======================

Follow the steps provided in :ref:`running-from-source`.
We are happy to accept pull requests and could use help
implementing the AtlasDB API for more underlying physical stores.

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

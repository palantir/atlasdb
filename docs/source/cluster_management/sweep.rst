.. _sweep:

AtlasDB Sweep Job
=================

.. warning::

   AtlasDB sweep is currently an experimental feature, and is disabled by default. If you are interested in testing it, please contact the AtlasDB team.

.. warning::

   Running the Sweep Job while taking a Backup can cause **SEVERE DATA CORRUPTION** to your Backup.


Under normal usage, the data written to the key value service backing AtlasDB is never updated or deleted.
When a row in AtlasDB is "updated", a new version of the row with a higher timestamp is written.
The old row is left untouched. When that row is later read, only the version with the higher timestamp is returned.
Deletions in AtlasDB are similar; in practice they are the same as updates with an empty value.

To prevent an AtlasDB database from growing in size indefinitely, old versions of rows can be deleted through a process called sweeping.
At a high level, this works by periodically scanning the database for rows with multiple versions and deleting the older versions from the database.


Reasons to Sweep
----------------

1. Freeing up Disk Space
    - Putting large amounts of data into cells that are updated creates tables that hold onto data that isn't used. These tables are good candidates to sweep if you want to reclaim disk space.

2. Improving Performance
    - Making many edits to the same row will leave behind many tombstoned entries, so it's advantageous to sweep these tables to increase performance.

Ways to Sweep
-------------

Under normal use, the sweep job is intended to run at a constant interval as a background process that does not consume significant system resources.
Older AtlasDB instances with high data scale and months/years of user activity will likely have a large number of accumulated cells that are eligible for sweeping (i.e. a lot of unused data in AtlasDB that has never been swept).
In this case, consider manually sweeping specific tables to reduce the number of unused cells that have accumulated over time.

Background Sweep
~~~~~~~~~~~~~~~~

For more information on how to set up a regular sweep job on your system, check out the :ref:`background_sweep` documentation.

Targeted Sweep
~~~~~~~~~~~~~~

If there are specific tables that have built up over time to a large size, you might consider running the targeted sweep job.
For more information about how to do this, check out our documentation on the :ref:`atlas-sweep-cli`.

.. toctree::
    :maxdepth: 1
    :hidden:

    sweep/background-sweep
    sweep/sweep-cli

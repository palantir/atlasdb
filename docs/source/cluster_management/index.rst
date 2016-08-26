==================
Cluster Management
==================

.. toctree::
   :maxdepth: 1
   :titlesonly:

   backup-restore
   clis
   console

You can add the Dropwizard bundle to **Dropwizard applications**. This will add startup commands to launch
the AtlasDB :ref:`Console <console>` and :ref:`CLIs <clis>` such as ``sweep`` and ``timestamp``,
which are needed to perform :ref:`live backups <backup-restore>`. **These commands will only work if
the server is started with a leader block in the configuration.**

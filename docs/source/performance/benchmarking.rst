==========================
Using The AtlasDB Perf Cli
==========================

1. Follow the steps provided in :ref:`running-from-source`.

.. note:: The Cassandra container in the above instructions has resource constraints and is not suitable for performance testing without modification.

2. Build the performance cli:

   .. code:: bash
        
        ./gradlew atlasdb-perf:installDist

3. Run the performance cli:

   .. code:: bash
        
        cd atlasdb-perf/build/install/atlasdb-perf/bin
        ./atlasdb-perf --help

.. warning:: Ensure that you are not running any additional docker containers that the tests will attempt to connect to!

.. _perf-cli:

==========================
Using The AtlasDB Perf Cli
==========================

1. Follow the steps provided in :ref:`running-from-source`.

.. note:: The Cassandra container in the above instructions has resource constraints and is not suitable for performance testing without modification. The heap size has been manually decreased in :code:`cassandra-env.sh`. Replace the line :code:`MAX_HEAP_SIZE="128M"`with the commented line directly above it and delete the line :code:`HEAP_NEWSIZE="24M"` to get more natural behavior.

Running from Command Line
=========================

2. Build the performance cli:

   .. code:: bash
        
        ./gradlew atlasdb-perf:installDist

3. Run the performance cli:

   .. code:: bash
        
        cd atlasdb-perf/build/install/atlasdb-perf/bin
        ./atlasdb-perf --help

Running in an IDE
=================

1. Generate the configuration files your your IDE (either `./gradlew idea` or `./gradlew eclipse`).

2. Run `AtlasDbPerfCli.java` (run with `--help` option for help).  You may need to add environment variables to the run configuration when using the `--backend` option to allow the program to communicate with a local docker instance.

.. note:: If you are getting unexpected behavior from your benchmark when running in an IDE, try deleting the generated_src directory and rebuilding. These files are not always automatically cleaned out when they should be.

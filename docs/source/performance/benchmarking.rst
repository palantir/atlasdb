=======================
Benchmarking in AtlasDB
=======================

The benchmarks are written using the `Java Benchmarking Harness <http://openjdk.java.net/projects/code-tools/jmh/>`__ (JMH) bundled with OpenJDK.  To get started understanding JMH take a look at the `JMH Samples <http://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/>`__.  Our benchmarks are found in the :code:`atlasdb-perf` project.

Running Benchmarks with the AtlasDB-perf CLI
============================================

1. Follow the steps provided in :ref:`running-from-source`.

2. Build the performance cli:

   .. code:: bash
        
        ./gradlew atlasdb-perf:installDist

3. Run the performance cli:

   .. code:: bash
        
        cd atlasdb-perf/build/install/atlasdb-perf/bin
        ./atlasdb-perf --help


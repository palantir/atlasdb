========================
Performance Benchmarking
========================

Running Benchmarks
==================

1. Follow the steps provided in :ref:`running-from-source`.

2. Build the performance cli:

   .. code:: bash
        
        ./gradlew atlasdb-perf:installDist
        cd atlasdb-perf/build/install/atlasdb-perf/bin

3. Run the performance cli:

   .. code:: bash
        
        ./atlasdb-perf --help

Contributing Benchmarks
=======================

The benchmarks are written using the `Java Benchmarking Harness <http://openjdk.java.net/projects/code-tools/jmh/>`__ (JMH) bundled with OpenJDK.  To get started understanding JMH take a look at the `JMH Samples <http://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/>`__.  Our benchmarks are found in the :code:`atlasdb-perf` project.

.. _performance-writing:

==================
Writing Benchmarks
==================

The benchmarks are written using the `Java Benchmarking Harness <http://openjdk.java.net/projects/code-tools/jmh/>`__ (JMH) bundled with OpenJDK.  To get started understanding JMH take a look at the `JMH Samples <http://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/>`__.  Our benchmarks are found in the ``atlasdb-perf`` project.

Be sure to read :ref:`Understanding Performance Benchmarking<understanding>` before attempting to write a benchmark.

How the AtlasDB-Perf CLI Works
==============================

``AtlasDbPerfCli.java`` contains the main method that the CLI runs.
It checks the provided options, sets up docker containers (if needed), and gathers the URIs of benchmarks via reflection.
Once it does all that, it kicks off JMH in the method ``AtlasDbPerfCli.runJmh()``.
This method is also responsible for setting several important runtime parameters including which benchmarks to run, how many forks and threads to use, the sampling mode, and the time unit.

.. note::
   If you want to run your benchmark in a debugger, you must set the number of forks to 0.
   Do not forget to set set it back to 1 before you push your code!

JMH will then attempt to run a benchmark.
Benchmark classes are all ``@State`` objects, which indicates that JMH can instantiate and manage them.
Each benchmark method is proceeded with a ``@Benchmark`` annotation, marking it as a benchmark.
Each Benchmark will also have a ``@Warmup`` and ``@Measurement`` annotation.
The Benchmark will be run repeatedly during the warmup and measurement period.
Warmup runs are not measured and are intended to warm the relevant caches such that runs in the measurement phase are more consistent.
During the measurement phase, the benchmark method executes as many times as it can with the allowed number of threads.
When JMH runs in SampleTime mode (see ``AtlasDbPerfCli.java``), a subset of the runs are timed.
JMH will sample as infrequently as it needs to in order to obtain accurate results.
Tests with a long runtime (>10ms) tend to be sampled every run.

.. tip::
   Set the ``@Measurement`` parameters high enough to get at least 100 runs.
   The CLI collects percentile statistics and with fewer than 100 samples this data is not particularly useful.

Benchmark method signatures can also indicate other ``@State`` objects, for example ``ConsecutiveNarrowTable``.
JMH will instantiate these for use in the benchmark.
State annotations specify a ``Scope`` which describes when the object is created and destroyed.
Similarly, methods in a State object can have functions marked as ``@Setup`` and ``@Teardown`` with a specified ``Level`` that describes when to run it.
Check the JMH source code for ``Scope`` and ``Level`` for more information.

Writing a New Benchmark
=======================

When writing a new benchmark, first consider exactly what you want to test.
If you are looking to test an API endpoint, it might be appropriate to write several benchmarks with each one covering a relevant case.
A good example is the ``KvsGetRangeBenchmarks.java`` suite, which tests AtlasDB's ``getRange()`` in several different ways.

Always include some form of validation in your benchmark.
Validation not only ensures that your code does what you think it does, it also protects against regressions in the benchmarks themselves which can be introduced by changing the underlying State objects.
Validation is often run during the benchmark itself, so care should be taken to avoid expensive operations in it.
Validation that does not include client/server calls generally has a negligible impact on benchmark runtime.

.. tip::
   Pass the name of the benchmark you're working on to the CLI in order to avoid running all benchmarks during development.

Benchmarking With a Custom Key Value Store (KVS)
================================================

To run the benchmark suite on a custom KVS, simply follow these steps:

1. Extend ``KeyValueServiceInstrumentation`` with the proper configurations, e.g., see ``CassandraKeyValueServiceInstrumentation.java``.
#. Register the new backend: ``KeyValueServiceInstrumentation.addNewBackendType(new YourKeyValueServiceInstrumentation())``.
#. Run the ``AtlasDbPerfCli.main`` function with the desired arguments.

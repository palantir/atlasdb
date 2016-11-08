.. _understanding:

======================================
Understanding Performance Benchmarking
======================================

AtlasDB includes a project dedicated to performance benchmarking. The goal is to prevent developers from regressing AtlasDB performance during development, but it can also be used to gather insight into how a code change or environment affects the performance of AtlasDB. 

Performance benchmarking can be a compelling, powerful development tool, but it is easy to misuse. There are many variables that can have a dramatic effect on a benchmark's results. Just because you are recieving output from our benchmarking tools does not mean you are gathering the information you think you are. This section summarizes some important considerations regarding performance testing and should not be skipped. 

The output of the AtlasDB-Performance Command Line Interface (AtlasDB-Perf CLI) is a function of the code, runtime environment, and benchmark design. The goal is to run an experiment where two of the three input variables are fixed in order to gain insight into how the third impacts performance. 

Environment
===========

The AtlasDB-Perf CLI does not assume anything about the environment it is run in and makes no efforts to restrict or configure it. It is crucial that you make careful, educated choices about where you are running the CLI and the database container it is connecting to. In general, you should attempt to simulate an environment as similar as possible to one a customer will be using. 

For the very best results:

1. Beware of running locally.

    Running the CLI on your local computer opens you up to a lot of interference. Other applications, background processes, and the network can create a lot of variance. It is also possible that you will encounter a different bottleneck than customers due to the very different hardware and resources on your local computer. If you optimize performance for your local setup, you might be regressing performance on customer hardware in a non-obvious way.

2. Be aware of the AWS VM type you are running on.

    A regression on an i2 instance can appear as a perf improvement on a m3.xlarge instance. The main consideration is i/o vs processor power. In general, try to run on AWS machines that are representitive of what customers might be running on. 

3. Do not run the CLI on the same VM as the the database unless you are simulating a colocated Client and server scenario.

    Not only does this setup ignore network considerations, it is also likely that the benchmark application will influence the performance of the database. In this case, changing non-benchmarked code in the benchmarking framework might have an unintended impact on your test results.

4. Consider running the CLI several times on multiple AWS VMs.

    Amazon does not make strict performance guarantees for a given VM type. That is to say, not all m4.xlarge VMs will have the exact same clock speed, hdd access speed, etc. If you want the most representitive data possible, you should run the same set of tests on multiple new boxes.


Benchmark Design
================

When writing or using a benchmark, take a moment to carefully consider what exactly it is testing. A given operation or API endpoint may have many cases that exhibit different performance characteristics. It may be that 99% of cases perform very well but 1% of cases are bad enough that the end user notices. In this instance, it may be worthwhile to compromise on the performance of the bulk of cases to improve the 1%. Most benchmarks only test a single case, so it is important to be fully aware which case that is. Gathering information on how exactly a feature is used before writing tests for it is often worthwhile.

Benchmarks in AtlasDB perform operations against tables that are created before the test using JMH State objects. We have attempted to create tables with characteristics that exercise various cases for AtlasDB's API. Some benchmarks run against relatively clean tables, while others run against tables with many rewrites. Some tests run against wide (many column) tables, and others run against narrow tables. 

Because we want our tests to run quickly and frequently, we have decided not to exhaustively run every operation against every table type on every backing KVS. Instead, we have hand-picked pairs of operations and table types to be run against each specified backing KVS. 

Please take a moment to read and understand some of the benchmarks before running them. 

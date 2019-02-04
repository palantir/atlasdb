## Overview
The ete benchmarks consist of two projects:
- **timelock-server-benchmark-client**: the benchmark client server.
Hosts a set of http endpoints which return benchmark results.
This server creates a `TransactionManager` which runs against the timelock cluster, and performs benchmarks on it.
- **timelock-server-benchmark-cluster**: the timelock server cluster.
This is just a normal timelock cluster used by the benchmark client to execute transactions.

There are two scripts for deploying the above projects:
- **push-benchmark-client.sh**: deploys the benchmark client server
- **push-benchmark-cluster.sh**: deploys a cluster of 3 timelock nodes

Finally, code for actually executing benchmarks is located in `BenchmarksRunner.java`.
This class simply hits endpoints on the benchmark server, and prints the output.

## Config and Hostnames
Configs for both the client and timelock cluster are controlled via templates, located in `var/conf/template`.
The purpose of these templates is to allow substitution of variables that should not be checked in.

Hostnames are configured via the `scripts/benchmarks/servers.txt` file.
The format is mostly self-explanatory; there are spots to configure the client node, the three timelock cluster nodes, and three cassandra nodes.
Finally, there is a variable for the keystore password; see below for details.

## Cassandra Setup & SSL
Your cassandra cluster should be configured to require client ssl.
The user should provide a keystore and a truststore to be used with cassandra and store them in the `scripts/benchmarks` folder.
The keystore password should be substituted in the `servers.txt` file mentioned above.
The suggested way to configure this is to use cassandra's keystore and the truststore, or use the certs from another service that was already configured.

## Host Setup
The scripts require your user to have access to the `/tmp/timelock-benchmark` directory, and to be able to view running processes.
This applies to both the benchmark cluster and client hosts.

## Running Benchmarks
To run the benchmarks:
1. Launch a cassandra cluster, and configure it per the "cassandra setup" section above
2. Launch four server boxes (3 for the timelock cluster + 1 for the benchmark client), and configure them per the "host setup" section above
3. Add the hostnames and keystore password to `scripts/benchmarks/servers.txt`
4. Run `./push-benchmark-cluster` and `./push-benchmark-client` to deploy the client and timelock cluster
5. Use `BenchmarksRunner.java` or curl the client to execute benchmarks.

For example, one can execute the following on the machine that is running the client:
`curl -kX GET "http://localhost:9425/perf/contended-write-txn?numClients=1&numRequestsPerClient=1"`

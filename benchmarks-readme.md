## Overview
The ete benchmarks consist of two projects:
- **timelock-server-benchmark-client**: the benchmark client server. Hosts a set of http endpoints which return benchmark results. This server creates a `TransactionManager` which runs against the timelock cluster, and performs benchmarks on it.
- **timelock-server-benchmark-cluster**: the timelock server cluster. This is just a normal timelock cluster used by the benchmark client to execute transactions

There are two scripts for deploying the above projects:
- **push-benchmark-client.sh**: deploys the benchmark client server
- **push-benchmark-cluster.sh**: deploys a cluster of 3 timelock nodes

Finally, code for actually executing benchmarks is located in `BenchmarksRunner.java`. This class simply hits endpoints on the benchmark server, and prints the output.

## Config and Hostnames
Configs for both the client and timelock cluster are controlled via templates, located in `var/conf/template`. The purpose of these templates is to allow substitution of the hostnames, so that the hostnames don't have to be checked in.

Hostnames are configured via the `scripts/benchmarks/servers.txt` file. The format is mostly self-explanatory; there are spots to configure the client node, the three timelock cluster nodes, and three cassandra nodes (if using cassandra)

## Host Setup
The scripts require your user to have access to the `/opt/palantir/timelock` directory, and to be able to view running processes. This applies to both the benchmark cluster and client hosts.

## Cassandra Setup
Your cassandra cluster should be configured to NOT require client ssl. This is set in `cassandra.yaml` via the `client_encryption_options.enabled` property (should be set to `false`). Alternatively you can enable ssl, but you'll need to modify the atlas configuration in `timelock-server-benchmark-client/var/conf/template/benchmark-server.yml` to use ssl for cassandra, and create a truststore.

## Running Benchmarks
To run the benchmarks:
1. Launch four server boxes (3 for the timelock cluster + 1 for the benchmark client), and configure them per the "host setup" section above
2. Launch a cassandra cluster, and configure it per the "cassandra setup" section above
3. Add the hostnames to `scripts/benchmarks/servers.txt`
4. Run `./push-benchmark-client` and `./push-benchmark-cluster` to deploy the client and timelock cluster
5. Use `BenchmarksRunner.java` to execute benchmarks

# 6. Create schema lock table using configuration

Date: 18/07/2016

## Status

Accepted

Supersedes [4. Create schema lock table via a one off CLI command](0004-create-schema-lock-table-via-a-one-off-cli-command.md)

Superseded by [PR #3620: Use CQL to create tables](https://github.com/palantir/atlasdb/pull/3620).

## Context

Due to [table creation issue](0002-prevent-tables-from-being-creating-simultaneously-in-cassandra-via-a-locks-table.md) we need to be able to safely create _lock table as a one off operation for each keyspace that atlas uses. The discussed options include:

- Have Atlas clients refuse to start if the _locks table is missing and provide a CLI to create it.
  - This does require manual interview for whoever is handling operations
  - Is very, very hard to get wrong
  - Should be easily automatable in most setups, removing the manual step

- Use the lock service for locking rather than the _locks table, then we don't have to create it.
  - Completely automatic and removes code
  - Leaves open the possibility of locks failing and operations never realising that the issue has been triggered

- Have each node create a unique table, then run paxos to decide which one is the winner
  - This requires a bunch of extra, error prone code

- Create an additional entry in the configuration, lockLeader, to denote which host is responsible for creating the locks table.
  - The host whose name is the same as lockLeader will create the lock table, others will wait until the lockLeader is up.
  - Requires all hosts to have the same configuration for lock leader

## Decision

We decided to use an extra item of configuration, because:
1. Internal tools enable us to be confident that different Atlas servers on the same cluster are configured consistently.
2. Running paxos to decide which table is the winner was more complex than anticipated.

## Consequences

On first-time startup, servers can now be started concurrently, but they must have consistent configuration.
There are two failure modes that can be caused by inconsistent configuration or failure of the lock leader:
1. No node believes that it is the lock leader - then all nodes will wait forever.
2. More than one node believes itself to be the lock table - then we will have duplicate lock tables and still be vulnerable to the initial issue.

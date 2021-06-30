4. Create schema lock table via a one off CLI command
*****************************************************

Date: 18/05/2016

## Status

Accepted

Superseded by [6. Create schema lock table using configuration](0006-create-schema-lock-table-using-configuration.md)

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


## Decision

We will implement the CLI based solution as:

- It is safe
- Requires a very simple set of code changes
- Should have minimal drawbacks if people can automate it

## Consequences

Deployers of atlas will have to run a CLI once on initial service installation.

# 5. stop allowing embedded lock and timestamp services in production
*********************************************************************

Date: 13/04/2016

## Status

Accepted

## Context

Currently a completely normal way to set up AtlasDB is to run an embedded lock and timestamp service that only your atlas client talks to in Java. If this happens with 2 different clients, then they can easily clash with each other and cause undefined issues in the KVS. It also stops CLIs that require knowledge of lock and timestamp being able to run against these atlas clients.

There is no reason for this situation to arise in production. Lock and Timestamp services can still be deployed in a sensible fashion without spinning up a separate service by registering the lock and timestamp server endpoints in your application, and having your own nodes act as the servers. Alternatively, you can spin up an completely external lock and timestamp service whose whole reason for existence is to perform these roles.

## Decision

1. Remove the ability to run embedded lock and timestamp services in configuration
2. Provide an easy to run Timelock Server which provides lock and timestamp services to applications
3. Provide a sensible development story for testing against a TimelockServer that can be started from Java.

## Consequences

1. We have to support a new, standalone server and all related considerations (e.g. versioning, healthchecks)

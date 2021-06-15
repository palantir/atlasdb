# 8. Adding Heartbeat for Schema Lock Holders
*********************************************

Date: 27/09/2016

## Status

Accepted

Superseded by [PR #3620: Use CQL to create tables](https://github.com/palantir/atlasdb/pull/3620).

## Context

As of version 0.16.0, anyone trying to acquire the a schema mutation lock that is already held by someone else
would keep trying to acquire the lock until a fixed timeout threshold. Once that threshold was reached, we would
stop trying to acquire the lock and throw a timeout exception. This would effectively kill the client trying to grab
the lock and necessitate a restart. We have a timeout threshold instead of retrying indefinitely to handle cases
where the lock holder dies without releasing the lock.

However, we think this has caused a lot of unnecessary exceptions since the threshold is static. We believe we could
avoid a lot of timeouts if we have some way of informing anyone trying to acquire the lock if the lock holder is alive
or dead. Once we have that information, we can keep retrying as long as we know that the lock holder is alive without
timing out needlessly. 

## Decision

To inform anyone trying to acquire the lock if the lock holder is dead or alive, we decided to have the lock holder 
periodically update its lock (heartbeat) in the locks table while its still holding the lock. That way, anyone else 
trying to acquire the lock can see the updates to the lock and keep retrying as long as the updates continue.

Currently, the lock value in the locks table consists of a randomly generated identifier. To incorporate the heartbeat 
updates into the same value, we decided to change the lock value to following format: 
`<randomly generated id>_<heartbeat value>`. The heartbeat value would essentially be incremented by 1 for every 
"heartbeat" by the lock holder.

For example, a lock with identifier `123456789` and heartbeat value `23` would have `123456789_23` as its lock value.

Once we had the heartbeat mechanism in place, we needed to decide whether to allow someone waiting on a lock
to override the lock and grab it once it detects that the lock holder is dead (via its heartbeat). Allowing locks
that could be overridden weakened the guarantee provided by the lock and could cause corruption in rare situations. 
On the plus side, overridable locks would minimize human intervention when dealing with dead lock holders.

However, it was decided that we should avoid the possibility of data corruption as much as possible. Therefore, we won't 
have overridable locks for now and simply use the information about the health of the lock holder to better inform
the exception we throw when we timeout on the lock. This will still require manual intervention (truncating the locks
table) when a lock holder dies without releasing the lock but provides a better picture of the situation
to the human user.

## Consequences

There should be significantly lower instances of schema mutation lock timeouts. The only cases where we should have 
timeouts should be:

1. When someone holding the lock dies, we throw an informative exception mentioning that we suspect the lock holder 
is dead and recommend cleaning the locks table.

2. When someone continues holding the lock for an **unreasonable** amount of time (default: 10 mins, is configurable)

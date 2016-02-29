---
title: Cassandra Configuration
---

## Overview

Make sure to run Cassandra with ByteOrderPartitioner (yes, I know everyone
disagrees, read below) and batch commit.  Without batch commit, your durability
and transactional guarantees are non-existent and you may as well just light
your data on fire.  When you enable batch commit, you must also bump up the
`concurrent_writes` param.  Cassandra holds every single row being written on
it's own thread until the batch sync.  This means you effectively need 1K
threads to get any write throughput.  

Again, you should edit `conf/cassandra.yaml`:

```yaml
partitioner: ByteOrderedPartitioner
commitlog_sync: batch
commitlog_sync_batch_window_in_ms: .1  # .1 is only appropriate for SSDs.  Use 5 for HDDs.
# If you use HDDs, make sure the commit log has it's own disk because it will thrash it.
concurrent_writes: 1024
```

## In Defense of ByteOrderedPartitioner

The Cassandra community has historically shunned the ByteOrderedPartitioner and
with good reasons. However I think that all the valid reasons were fixed in 1.2
once vnodes were implemented.  The documentation and blogs have not evolved to
reflect that ByteOrderedParitioner is now the right default choice.

Yes, I know this is a very unpopular opinion. Here are some good quotes that
show what I’m up against (emphasis is mine).

> [<span style="color: #555555;">we [..] looked at [...]</span>
_RandomPartitioner_ <span style="color: #555555;">and</span>
_ByteOrderedPartitioner [..]_<span style="color: #555555;">. We reached the
conclusion **Random Partitioner should always be preferred** over any type of
Ordered
Partitioning</span>](http://10kloc.wordpress.com/2012/12/27/cassandra-chapter-4-data-partitioning/)

> [<span style="color: #1a1a1a;">So to sum this up: The Internet is right,
**don’t use the
ByteOrderedPartitioner**</span>](http://www.geroba.com/cassandra/apache-cassandra-byteorderedpartitioner/)

Looking more closely, the downsides boil down to 2 points. The first is that
the ring will be hard to maintain because your data won’t be randomly
distributed. The second is that there will be hotspots due to your data not
being randomly distributed. The worry is that if you just insert sequential row
names you will basically get single node perf for reads and writes.

I think both points are good starting advice due to hotspots. However there are
other systems out there like HBase that already force you to think about this
when you design a schema. I think choosing RandomPartitioner limits your
options because range scans are no longer possible. If you really want your
keys distributed randomly, you can do so by prepending a hash to each key.

The main issue is that this setting is global in Cassandra. If you want range
scans for a single table, then you must use ByteOrderedParitioner.

I think with good schema design and thinking about your access patterns then
ByteOrderedPartitioner is a good default choice. If you have a specific table
where you need it to be randomly distributed, then you can add a 2 byte hash to
the front of each key.

In atlas this can be done by using `ValueType.FIXED_LONG_LITTLE_ENDIAN`.  This
value type reverses the bytes of a long so that the least significant byte is
the first one.  Assuming your keys are evenly distributed in the least
significant bytes, this will get you good distribution even with the
ByteOrderedPartitioner.

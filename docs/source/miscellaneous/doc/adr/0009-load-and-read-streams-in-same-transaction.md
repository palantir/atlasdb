9. Load and Read Streams in the Same Transaction
************************************************

Date: 20/01/2017

## Status

Proposed

## Context

Reading streams from within the same transaction that calls loadStream() must
be supported. There are a number of benefits to doing this rather than reading
streams outside the transaction.

1. If the transaction that calls loadStream() is run with retry, you risk
   leaking resources because multiple streams may be created that never get
   returned to the caller (so the caller has no opportunity to close them).
   The stream can not only be read (entirely) inside the transaction, but
   also closed inside the transaction. This makes it safe to run with retries
   (since the retries can also close the stream they load).

2. When the actual stream data is read outside of the transaction, that read
   can fail (for example if the stream was then deleted). If instead the read is
   performed inside the transaction, any failed reads would also cause the
   transaction to fail, which could be desired. For example, if as a result of
   reading the stream you want to perform some writes based on the read data, it
   makes sense to perform all of that in a single transaction.

## Decision

Reading from a stream inside the same transaction that calls loadStream()
should be the preferred method unless you have a specific reason that the reads
must be performed later outside the transaction. Reading from a stream should
be supported both inside the same transaction where it was loaded and outside
(after the transaction commits or aborts).


## Consequences

Internal products currently rely on being able to read streams in the same
transaction that loadStream() is called. This must be supported unless you
first ensure that all internal products have been rewritten to never read
from a stream in the same transaction that loads it.

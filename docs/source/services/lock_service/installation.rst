============
Installation
============

How to choose a number of lock servers
--------------------------------------

Distributed consensus is not useful for clusters of size one and two.
It's generally advisable to choose 3-6 lock servers, because too many lock servers will negatively impact performance
owing to the overhead of distributed consensus and leader election.
You should not see much of a performance difference within the range [3, 4, 5, 6].

Note that we usually prefer to select an odd number of servers, because as far as obtaining a quorum is concerned,
that has the same tolerance to node failures as the next higher even number. However, selecting an even number does
have advantages under certain circumstances, such as tolerance to node failures with loss of disk logs.

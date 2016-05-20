============
Installation
============

How to choose a number of lock servers
--------------------------------------

Distributed consensus is not useful for clusters of size one and two.
It's generally advisable to choose 3-6 lock servers, because too many lock servers will negatively impact performance.
You should not see much of a performance difference within the range [3, 4, 5, 6]

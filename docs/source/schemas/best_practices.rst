=======
Best Practices
=======

Overview
========

This section provides some guidelines for how to think about schema structure and query patterns.

Range Scans
===============

* Range Scans vs  Exact Key Lookups:
    * Prefer exact key lookups over range scans
        * The performance of exact-key lookups does not degrade with frequent updates to the key



* Row-based vs Dynamic column-based range scans
    * Prefer dynamic column range scans
        * Range scanning over dynamic columns within a row can be 2 or more times faster than range scanning across rows



* Range scan caveats
    * Do not range scan across rows where data is updated / overwritten / deleted
        * A range scan across row keys will fetch all historical versions of every cell in every row. This means that range scan performance degrades linearly with the number of updates to cells in the range.
        * This can be partially alleviated by sweep, but you should design your schema in such a way that sweep is not strictly required to maintain good performance
    * Do not attempt to “filter” row keys by selecting only one column in your range scan
        * Atlas will retrieve all column values for every row returned in a range scan, even if only one column is requested.
    * Consider the impact on data distribution when designing a table for range scans
        * Range scans inherently lead to hotspotting, because keys that are close to one another in a range will end up on the same Cassandra node.
    * When using dynamic columns for range scans, do not allow the total size of your row to grow above ~100MB.
        * Large rows reduce performance of compactions and create heap pressure in Cassandra

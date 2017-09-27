=======
Best Practices
=======




Range Scans
===============

**Range Scans vs  Exact Key Lookups**

- Prefer exact key lookups over range scans
   - The performance of exact key lookups do not degrade with frequent updates to the key
   - They will never fetch more data than you request


**Row vs Dynamic Column Range Scans**

- Prefer dynamic column range scans
   - Range scanning over dynamic columns within a row can be 2 or more times faster than range scanning across rows
   - Dynamic column scans are more efficient, and Cassandra is optimized for them


**Range Scan Caveats**

- Avoid range scanning across rows or dynamic columns where data is updated / overwritten / deleted
   - Range scans will fetch all historical versions of every cell. This means that range scan performance degrades linearly with the number of updates to cells in the range
   - The impact of overwrites can be alleviated over time by background sweep, but in general, you should design your schema in such a way that your queries do not rely on sweep to maintain good performance
- Do not attempt to “filter” row keys by selecting only one column in your range scan
   - Atlas will retrieve all column values for every row returned in a range scan, even if only one column is requested.
- Consider the impact on data distribution when designing a table for range scans
   - Range scans inherently lead to hotspotting, because keys that are close to one another in a range will end up on the same Cassandra node.
- When using dynamic columns for range scans, do not allow the total size of your row to grow above ~100MB.
   - Large rows reduce performance of compactions and create heap pressure in Cassandra

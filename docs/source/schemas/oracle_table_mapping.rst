====================
Oracle Table Mapping
====================

Oracle now has a table mapper that will shorten all names more than 30 characters long. This uses a heuristic
mechanism to generate readable and unique table names:

- The last six characters are reserved for a sequence number.
- The prefix remains unchanged.
- We truncate the namespace to 2 characters appended by a dunderscore.
- We truncate the table name to fit in the remaining characters. ie (30 - tablePrefixLength - namespaceLength - sequenceNumberLength).

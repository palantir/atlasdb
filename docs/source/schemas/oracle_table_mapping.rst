====================
Oracle Table Mapping
====================

Oracle now has a table mapper that will shorten all names more than 30 characters long. This uses a heuristic
mechanism to generate readable and unique table names:

- The last five characters are reserved for a SHA-256 hash on the table name.
- The prefix remains unchanged.
- So, we fit the table name in ``30 - (5 + prefix length)`` characters. This is how it is done:

    - Drop vowels from the end of the table name until it fits.
    - Truncate the table name, if it still exceeds.


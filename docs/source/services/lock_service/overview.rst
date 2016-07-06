========
Overview
========

The Lock Service
----------------

The lock service helps prevent data corruption in the database by enabling clients to avoid modifying the same data at the same time.

The lock service listens for lock requests.
A lock request is what we call a client asking for permission to modify data in the database.
The lock service will receive the lock request from the client, and will consider whether it should approve the request.

The lock service determines whether to approve a request by checking an internal hash table of granted lock requests.
If the resource to be locked is already in the hash table, the lock service will not approve the request.
When the resource becomes available, the lock service will approve the request, and send a lock token to the client.

The lock token is essentially an indicator that the client should proceed with its transaction.
However, the lock token is not actually a mutex; it is just a token.

.. note::
   Lock clients are expected to abide by these locks as per the :ref:`transaction-protocol`.


The Lock Client
---------------

A lock client runs on any service that wishes to use the lock service to coordinate activity on the database.
The lock client uses tokens from the lock service to determine whether it is safe to proceed with transactions.

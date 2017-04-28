.. _kvs-status-check:

======================
KeyValueService Status
======================

Call ``TransactionManagers.getKeyValueServiceStatus()`` to find the state of the underlying key value service.
Here are the values that can be returned by the status API.

* **TERMINAL** : Implies the KVS configuration does not match the key value store state.
* **UNHEALTHY** : Implies the KVS cannot perform read/write operations.
* **HEALTHY_BUT_NO_SCHEMA_MUTATIONS_OR_DELETES**: Implies read/write operations are available but the KVS cannot perform deletes or schema mutations.
* **HEALTHY_ALL_OPERATIONS**: Implies the KVS is healthy and all operations are available.

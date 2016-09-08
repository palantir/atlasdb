.. _change-log:

*********
Changelog
*********

.. role:: changetype
.. role:: changetype-breaking
    :class: changetype changetype-breaking
.. role:: changetype-new
    :class: changetype changetype-new
.. role:: changetype-fixed
    :class: changetype changetype-fixed
.. role:: changetype-changed
    :class: changetype changetype-changed
.. role:: changetype-improved
    :class: changetype changetype-improved
.. role:: changetype-deprecated
    :class: changetype changetype-deprecated

.. |breaking| replace:: :changetype-breaking:`BREAKING`
.. |new| replace:: :changetype-new:`NEW`
.. |fixed| replace:: :changetype-fixed:`FIXED`
.. |changed| replace:: :changetype-changed:`CHANGED`
.. |improved| replace:: :changetype-improved:`IMPROVED`
.. |deprecated| replace:: :changetype-deprecated:`DEPRECATED`

.. toctree::
  :hidden:

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.15.0
=======

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |new|
         - Added another method to page through dynamic columns which returns results in a single iterator. This is expected
           to perform better than the existing method with certain backing stores (e.g. SQL stores), so should be preferred
           unless it is actually important to be able to page through the results for different rows separately.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/724>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.14.0
=======

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |breaking|
         - ``TransactionManagers.create()`` no longer takes in an argument of ``Optional<SSLSocketFactory> sslSocketFactory``.
           Instead, security settings between AtlasDB clients are now specified directly in configuration via the new optional parameter ``sslConfiguration`` located in the ``leader`` block.
           Details can be found in the :ref:`Leader Configuration <leader-config>` documentation.
           This will only affect deployments who run with more than one server (i.e. in HA mode).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/873>`__)

    *    - |breaking|
         - Enforced validity constraints on configuration, as per `#790 <https://github.com/palantir/atlasdb/issue/790>`__.
           AtlasDB will now fail to start if your configuration is invalid.
           Please refer to :ref:`Example Leader Configurations <leader-config-examples>` for guidance on valid configurations.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/854>`__)

    *    - |new|
         - ``CassandraKeyValueServiceConfiguration`` now supports :ref:`column paging <cassandra-sweep-config>`
           via the ``timestampsGetterBatchSize`` parameter.

           Enabling such paging could make :ref:`Sweep <physical-cleanup-sweep>` more reliable by helping
           prevent sweep jobs from causing Cassandra nodes to run out of memory if the underlying Cassandra
           KVS contains rows that store large values and change frequently.

           This feature is experimental and disabled by default; please
           reach out to the AtlasDB dev team if you would like to enable it.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/834>`__)

    *    - |fixed|
         - Fixed and standardized serialization and deserialization of AtlasDBConfig.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/875>`__)

    *    - |fixed|
         - Updated our Dagger dependency from 2.0.2 to 2.4, so that our generated code matches with that of internal products.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/878>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.13.0
=======

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |breaking|
         - ``AtlasDbServer`` has been renamed to ``AtlasDbServiceServer``.
           Any products that are using this should switch to using the standard AtlasDB java API instead.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/801>`__)

    *    - |breaking|
         - The method ``updateManyUnregisteredQuery(String sql)`` has been removed from the ``SqlConnection`` interface, as it was broken, unused, and unnecessary.
           Use ``updateManyUnregisteredQuery(String sql, Iterable<Object[] list>)`` instead.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/796>`__)

    *    - |improved|
         - Improved logging for schema mutation lock timeouts and added logging for obtaining and releasing locks.
           Removed the advice to restart the client, as it will not help in this scenario.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/805>`__)

    *    - |fixed|
         - Connections to Cassandra can be established over arbitrary ports.
           Previously AtlasDB clients would assume the default Cassandra port of 9160 despite what is specified in the :ref:`Cassandra keyValueService configuration <cassandra-kvs-config>`.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/771>`__)

    *    - |fixed|
         - Fixed an issue when starting an AtlasDB client using the Cassandra KVS where we always grab the schema mutation lock, even if we are not making schema mutations.
           This reduces the likelihood of clients losing the schema mutation lock and having to manually truncate the _locks table.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/771>`__)

    *    - |improved|
         - Performance and reliability enhancements to the in-beta CQL KVS.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/771>`__)


.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.12.0
=======

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |breaking|
         - AtlasDB will always try to register timestamp and lock endpoints for your application, whereas previously this only occurred if you specify a :ref:`leader-config`.
           This ensures that CLIs will be able to run against your service even in the single node case.
           For Dropwizard applications, this is only a breaking change if you try to initialize your KeyValueService after having initialized the Dropwizard application.
           Note: If you are initializing the KVS post-Dropwizard initialization, then your application will already fail when starting multiple AtlasDB clients.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/708>`__)

    *    - |new|
         - There is now a Dropwizard bundle which can be added to Dropwizard applications.
           This will add startup commands to launch the AtlasDB console and :ref:`CLIs <clis>` suchs as ``sweep`` and ``timestamp``, which is needed to perform :ref:`live backups <backup-restore>`.
           These commands will only work if the server is started with a leader block in its configuration.
           (`Pull Request 1 <https://github.com/palantir/atlasdb/pull/629>`__ and `Pull Request 2 <https://github.com/palantir/atlasdb/pull/696>`__)

    *    - |fixed|
         - DB passwords are no longer output as part of the connection configuration ``toString()`` methods.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/755>`__)

    *    - |new|
         - All KVSs now come wrapped with ProfilingKeyValueService, which at the TRACE level provides timing information per KVS operation performed by AtlasDB.
           See :ref:`logging-configuration` for more details.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/798>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.11.4
=======

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Correctly checks the Cassandra client version that determines if Cassandra supports Check And Set operations.
           This is a critical bug fix that ensures we actually use our implementation from `#436 <https://github.com/palantir/atlasdb/pull/436>`__, which prevents the Cassandra concurrent table creation bug described in `#431 <https://github.com/palantir/atlasdb/issues/431>`__.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/751>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.11.2
=======

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |changed|
         - The ``ssl`` property now takes precedence over the new ``sslConfiguration`` block to allow back-compatibility.
           This means that products can add default truststore and keystore configuration to their AtlasDB config without overriding previously made SSL decisions (setting ``ssl: false`` should cause SSL to not be used).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/745>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.11.1
=======

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Removed a check enforcing a leader block config when one was not required.
           This prevents AtlasDB 0.11.0 clients from starting if a leader configuration is not specified (i.e. single node clusters).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/741>`__)

    *    - |improved|
         - Updated schema table generation to optimize reads with no ColumnSelection specified against tables with fixed columns.  To benefit from this improvement you will need to re-generate your schemas.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/713>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.11.0
=======

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - Clarified the logging when multiple timestamp servers are running to state that CLIs could be causing the issue.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/719>`__)

    *    - |changed|
         - Updated cassandra client from 2.2.1 to 2.2.7 and cassandra docker testing version from 2.2.6 to 2.2.7.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/699>`__)

    *    - |fixed|
         - The leader config now contains a new ``lockCreator`` option, which specifies the single node that creates the locks table when starting your cluster for the very first time.
           This configuration prevents an extremely unlikely race condition where multiple clients can create the locks table simultaneously.
           Full details on the failure scenario can be found on `#444 <https://github.com/palantir/atlasdb/issues/444#issuecomment-221612886>`__.

           If left blank, ``lockCreator`` will default to the first host in the ``leaders`` list, but we recommend setting this explicitly to ensure that the lockCreater is the same value across all your clients for a specific service.
           This configuration is only relevant for new clusters and does not affect existing AtlasDB clusters.

           Full details for configuring the leader block, see `cassandra configuration <https://palantir.github.io/atlasdb/html/configuration/cassandra_KVS_configuration.html>`__.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/594>`__)

    *    - |fixed|
         - A utility method was removed in the previous release, breaking an internal product that relied on it.
           This method has now been added back.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/661>`__)

    *    - |fixed|
         - Removed unnecessary error message for missing _timestamp metadata table.
           _timestamp is a hidden table, and it is expected that _timestamp metadata should not be retrievable from public API.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/716>`__)

    *    - |improved|
         - Trace logging is more informative and will log all failed calls.
           To enable trace logging, see `Enabling Cassandra Tracing <https://palantir.github.io/atlasdb/html/configuration/enabling_cassandra_tracing.html#enabling-cassandra-tracing>`__.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/700>`__)

    *    - |deprecated|
         - The Cassandra KVS now supports specifying SSL options via the new ``sslConfiguration`` block, which takes precedence over the now deprecated ``ssl`` property.
           The ``ssl`` property will be removed in a future release, and consumers leveraging the Cassandra KVS are encouraged to use the ``sslConfiguration`` block instead.
           See the `Cassandra SSL Configuration <https://palantir.github.io/atlasdb/html/configuration/cassandra_KVS_configuration.html#communicating-over-ssl>`__ documentation for more details.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/638>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.10.0
=======

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |changed|
         - Updated HikariCP dependency from 2.4.3 to 2.4.7 to comply with updates in internal products.
           Details of the HikariCP changes can be found `here <https://github.com/brettwooldridge/HikariCP/blob/dev/CHANGES>`__.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/662>`__)

    *    - |new|
         - AtlasDB currently allows you to create dynamic columns (wide rows), but you can only retrieve entire rows or specific columns.
           Typically with dynamic columns, you do not know all the columns you have in advance, and this features allows you to page through dynamic columns per row, reducing pressure on the underlying KVS.
           Products or clients (such as AtlasDB Sweep) making use of wide rows should consider using ``getRowsColumnRange`` instead of ``getRows`` in ``KeyValueService``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/582>`__)

           Note: This is considered a beta feature and is not yet being used by AtlasDB Sweep.

    *    - |fixed|
         - We properly check that cells are not set to empty (zero-byte) or null.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/663>`__)

    *    - |improved|
         - Cassandra client connection pooling will now evict idle connections over a longer period of time and has improved logic
           for deciding whether or not a node should be blacklisted.  This should result in less connection churn
           and therefore lower latency.  (`Pull Request <https://github.com/palantir/atlasdb/pull/667>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

======
v0.9.0
======

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |breaking|
         - Inserting an empty (size = 0) value into a ``Cell`` will now throw an ``IllegalArgumentException``. (`#156 <https://github.com/palantir/atlasdb/issues/156>`__) Likely empty
           values include empty strings and empty protobufs.

           AtlasDB cannot currently distinguish between empty and deleted cells. In previous versions of AtlasDB, inserting
           an empty value into a ``Cell`` would delete that cell. Thus, in this snippet,

           .. code-block:: java

               Transaction.put(table, ImmutableMap.of(myCell, new byte[0]))
               Transaction.get(table, ImmutableSet.of(myCell)).get(myCell)

           the second line will return ``null`` instead of a zero-length byte array.

           To minimize confusion, we explicitly disallow inserting an empty value into a cell by throwing an
           ``IllegalArgumentException``.

           In particular, this change will break calls to ``Transaction.put(TableReference tableRef, Map<Cell, byte[]> values)``,
           as well as generated code which uses this method, if any entry in ``values`` contains a zero-byte array. If your
           product does not need to distinguish between empty and non-existent values, simply make sure all the ``values``
           entries have positive length. If the distinction is necessary, you will need to explicitly differentiate the
           two cases (for example, by introducing a sentinel value for empty cells).

           If any code deletes cells by calling ``Transaction.put(...)`` with an empty array, use
           ``Transaction.delete(...)`` instead.

           *Note*: Existing cells with empty values will be interpreted as deleted cells, and will not lead to Exceptions when read.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/524>`__)

    *    - |improved|
         - The warning emitted when an attempted leadership election fails is now more descriptive.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/630>`__)

    *    - |fixed|
         - Code generation for the ``hashCode`` of ``*IdxColumn`` classes now uses ``deepHashCode`` for its arrays such that it returns
           consistent hash codes for use with hash-based collections (HashMap, HashSet, HashTable).
           This issue will only appear if you are instantiating columns in multiple places and storing columns in hash collections.

           If you are using `Indices <https://palantir.github.io/atlasdb/html/schemas/tables_and_indices.html#indices>`__ we recommend you upgrade as a precaution and ensure you are not relying on logic related to the ``hashCode`` of auto-generated ``*IdxColumn`` classes.
           You will need to regenerate your schema code in order to see this fix.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/600>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

======
v0.8.0
======

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Some logging was missing important information due to use of the wrong substitution placeholder. This version should be taken in preference to 0.7.0 to ensure logging is correct.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/642>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

======
v0.7.0
======

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |new|
         - AtlasDB can now be backed by Postgres via DB KVS. This is a very early release for this feature, so please contact us if you
           plan on using it. Please see :ref:`the documentation <postgres-configuration>` for more details.

    *    - |fixed|
         - The In Memory Key Value Service now makes defensive copies of any data stored or retrieved. This may lead to a slight performance degradation to users of In Memory Key Value Service.
           In Memory Key Value Service is recommended for testing environments only and production instances should use DB KVS or Cassandra KVS for data that needs to be persisted.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/552>`__)

    *    - |fixed|
         - AtlasDB will no longer log incorrect errors stating "Couldn't grab new token ranges for token aware cassandra mapping" when running against a single node and single token Cassandra cluster.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/634>`__)

    *    - |improved|
         - Read heavy workflows with Cassandra KVS will now use substantially less heap. In worst-case testing this change resulted in a 10-100x reduction in client side heap size.
           However, this is very dependent on the particular scenario AtlasDB is being used in and most consumers should not expect a difference of this size.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/568>`__)

.. <<<<------------------------------------------------------------------------------------------------------------>>>>

======
v0.6.0
======

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *   - Type
        - Change

    *   - |fixed|
        - A potential race condition could cause timestamp allocation to never complete on a particular node (#462)

    *   - |fixed|
        - An innocuous error was logged once for each TransactionManager about not being able to allocate
          enough timestamps. The error has been downgraded to INFO and made less scary.

    *   - |fixed|
        - Serializable Transactions that read a column selection could consistently report conflicts when there were none.

    *   - |fixed|
        - An excessively long Cassandra related logline was sometimes printed (#501)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

======
v0.5.0
======

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *   - Type
        - Change

    *   - |changed|
        - Only bumping double minor version in artifacts for long-term stability fixes

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

======
v0.4.1
======

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *   - Type
        - Change

    *   - |fixed|
        - Prevent _metadata tables from triggering the Cassandra 2.x schema mutation bug `431 <https://github.com/palantir/atlasdb/issues/431>`_ (`444 <https://github.com/palantir/atlasdb/issues/444>`_ not yet fixed)

    *   - |fixed|
        - Required projects are now Java 6 compliant


.. <<<<------------------------------------------------------------------------------------------------------------->>>>

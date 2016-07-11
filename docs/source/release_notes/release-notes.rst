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

.. |breaking| replace:: :changetype-breaking:`BREAKING`
.. |new| replace:: :changetype-new:`NEW`
.. |fixed| replace:: :changetype-fixed:`FIXED`
.. |changed| replace:: :changetype-changed:`CHANGED`
.. |improved| replace:: :changetype-improved:`IMPROVED`

.. toctree::
  :hidden:

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.9.0
=======

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |breaking|
         - Inserting an empty (size = 0) value into a ``Cell`` will now throw an ``IllegalArgumentException``. (`#156 <https://github.com/palantir/atlasdb/issues/156>`__)) Likely empty
           values include empty strings and empty protobufs.

           Atlas cannot currently distinguish between empty and deleted cells. In previous versions of Atlas, inserting
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
           correct hash codes.

           *Note*: you will need to regenerate your schema code in order to see this fix.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/600>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.8.0
=======

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Some logging was missing important information due to use of the wrong substitution placeholder. This version should be taken in preference to 0.7.0 to ensure logging is correct.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/642>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.7.0
=======

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |new|
         - Atlas can now be backed by Postgres via DB KVS. This is a very early release for this feature, so please contact us if you
           plan on using it. Please see `the documentation <http://palantir.github.io/atlasdb/html/configuration/postgres_key_value_service_config.html>`_ for more details.

    *    - |fixed|
         - The In Memory Key Value Service now makes defensive copies of any data stored or retrieved. This may lead to a slight performance degradation to users of In Memory Key Value Service.
           In Memory Key Value Service is recommended for testing environments only and production instances should use DB KVS or Cassandra KVS for data that needs to be persisted.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/552>`__)

    *    - |fixed|
         - Atlas will no longer log incorrect errors stating "Couldn't grab new token ranges for token aware cassandra mapping" when running against a single node and single token Cassandra cluster.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/634>`__)

    *    - |improved|
         - Read heavy workflows with Cassandra KVS will now use substantially less heap. In worst-case testing this change resulted in a 10-100x reduction in client side heap size.
           However, this is very dependent on the particular scenario AtlasDB is being used in and most consumers should not expect a difference of this size.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/568>`__)

.. <<<<------------------------------------------------------------------------------------------------------------>>>>

=======
v0.6.0
=======

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

=======
v0.5.0
=======

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *   - Type
        - Change

    *   - |changed|
        - Only bumping double minor version in artifacts for long-term stability fixes

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.4.1
=======

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

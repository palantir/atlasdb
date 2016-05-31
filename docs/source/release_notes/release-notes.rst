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
v0.7.0
=======

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *   - Type
        - Change

    *   - |breaking|
        - Inserting an empty (size = 0) value into a ``Cell`` will now throw an ``IllegalArgumentException``. (#156)

          Atlas cannot currently distinguish between empty and deleted cells. In previous versions of Atlas, inserting
          an empty value into a ``Cell`` would delete that cell. Thus, in this snippet,

          .. code-block:: java

              Transaction.put(table, ImmutableMap.of(myCell, new byte[0]))
              Transaction.get(table, ImmutableSet.of(myCell)).get(myCell)

          the second line will return ``null`` instead of a zero-length byte array.

          To minimize confusion, we explicitly disallow inserting an empty value into a cell by throwing an
          ``IllegalArgumentException``.

          In particular, this change will break calls to ``Transaction.put(TableReference tableRef, Map<Cell, byte[]> values)``
          if any entry in ``values`` contains a zero-byte array. If your product does not need to distinguish between
          empty and non-existent values, simply make sure all the ``values`` entries have positive length. If the
          distinction is necessary, you will need to explicitly differentiate the two cases (for example, by introducing
          a sentinel value for empty cells).

          If any code deletes cells by calling ``Transaction.put(...)`` with an empty array, use
          ``Transaction.delete(...)`` instead.


.. <<<<------------------------------------------------------------------------------------------------------------->>>>

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

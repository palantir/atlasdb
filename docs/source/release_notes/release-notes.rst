*********
Changelog
*********

.. role:: changetype
.. role:: changetype-new
    :class: changetype changetype-new
.. role:: changetype-fixed
    :class: changetype changetype-fixed
.. role:: changetype-changed
    :class: changetype changetype-changed
.. role:: changetype-improved
    :class: changetype changetype-improved

.. |new| replace:: :changetype-new:`NEW`
.. |fixed| replace:: :changetype-fixed:`FIXED`
.. |changed| replace:: :changetype-changed:`CHANGED`
.. |improved| replace:: :changetype-improved:`IMPROVED`

.. toctree::
  :hidden:

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

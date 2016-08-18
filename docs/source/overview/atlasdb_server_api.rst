.. _atlasdb-server-api:

==================
AtlasDB Server Api
==================

Table Requests
==============

Create a Table
--------------

.. code:: sh

    curl -XPOST http://localhost:3828/atlasdb/create-table/my_table

Puts and Gets (Auto-Committed)
------------------------------

.. code:: sh

    curl -XPOST http://localhost/atlasdb/put/auto-commit -d'{"table":"my_table","data":[{"row":["AAEC"],"col":["AwQF"],"val":"KAA="}]}'

.. code:: sh

    curl -XPOST http://localhost/atlasdb/cells/auto-commit -d'{"table":"my_table","data":[{"row":["AAEC"],"col":["AwQF"]}]}'

Transactions
============

Open a Transaction
------------------

.. code:: sh

    curl -XPOST http://localhost:3828/atlasdb/transaction

returns this

.. code:: json

    {"id":"14f0656a-e5f3-48d7-a15e-6fa3504db797"}

Read and Write Transactionally
------------------------------

.. code:: sh

    curl -XPOST http://localhost/atlasdb/put/14f0656a-e5f3-48d7-a15e-6fa3504db797 -d'{"table":"my_table","data":[{"row":["AAEC"],"col":["AwQF"],"val":"KAA="}]}'

.. code:: sh

    curl -XPOST http://localhost/atlasdb/cells/14f0656a-e5f3-48d7-a15e-6fa3504db797 -d'{"table":"my_table","data":[{"row":["AAEC"],"col":["AwQF"]}]}'

Commit a Transaction
--------------------

.. code:: sh

    curl -XPOST http://localhost:3828/atlasdb/commit/14f0656a-e5f3-48d7-a15e-6fa3504db797

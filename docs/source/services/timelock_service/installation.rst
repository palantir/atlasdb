.. _timelock-installation:

Timelock Server Installation
============================

This guide explains how to install the Timelock server, and to configure existing services to use Timelock Server rather than embedded AtlasDB time and lock services.
The basic method is to first install Timelock without any clients, and then for each existing service, prepare the service and Timelock to talk to each other.

Obtain the Timelock binaries
----------------------------

.. tip::

   Internal consumers can skip these steps.

1. Clone the git repository.

   .. code:: bash

      git clone git@github.com:palantir/atlasdb.git; cd atlasdb

2. Generate a tar file with Gradle:

   .. code:: bash

      ./gradlew timelock-server:distTar

  This will place a tar file in the ``build/distributions`` directory of the ``timelock-server`` project. The
  tar file follows semantic versioning.

Install Timelock (without clients)
----------------------------------

We recommend deploying either a 3-node or a 5-node Timelock cluster.
A 3-node cluster will provide better performance, whereas a 5-node cluster has greater fault tolerance.

.. tip::

   Internal consumers can deploy the binaries by the usual method.

For real-world installations, we recommend provisioning a dedicated host machine for each Timelock server.

3. Copy this tar file to each machine on which you want to deploy Timelock Server.

4. On each machine which you intend to deploy Timelock Server, untar the archive.

   .. code:: bash

      tar -zxvf timelock-server-0.25.0.sls.tgz; cd timelock-server-0.25.0

5. Configure the Timelock Server - see :ref:`timelock-server-configuration` for a guide on this. The configuration file
   is at ``var/conf/timelock.yml`` relative to the root directory of the Timelock Server.

6. Start the Timelock Server with ``service/bin/init.sh start``.
   This should output the process ID of the Timelock Server. You can view the logs in the (by default) ``var/log``
   directory.

7. It may be useful to run a health-check to verify that the Timelock Server is running. To do this, you can curl
   the server's ``healthcheck`` endpoint on its admin port (default: 8081).

   .. code:: bash

      curl localhost:8081/healthcheck

   The output should indicate that the Timelock Server is healthy. The output should resemble the following:

   .. code:: bash

      {
          "deadlocks": {
              "healthy": true
          }
      }

Add client(s) to Timelock
-------------------------

8. Back up each client

9. Configure each client to use Timelock.
   Detailed documentation is :ref:`here <timelock-client-configuration>`.
   You must remove any ``leader``, ``timestamp``, or ``lock`` blocks; the ``timelock`` block to add looks like this:

.. code-block:: yaml

   atlasdb:
      timelock:
        serversList:
          servers:
            - palantir-1.com:8080
            - palantir-2.com:8080
            - palantir-3.com:8080
          sslConfiguration:
            trustStorePath: var/security/truststore.jks

10. (Optional) For verification purposes, you may retrieve a timestamp from each client you are configuring to use TimeLock.
    This can typically be performed with the Fetch Timestamp CLI or Dropwizard bundle. For example, using the Dropwizard bundle:

.. code-block:: bash

   ./service/bin/<service> atlasdb timestamp fetch

    Note down the value of the timestamp returned; we will subsequently use these values to ensure that migration took place.

11. Shut down each client that has been newly added.

12. Restart your Timelock cluster.

13. Migrate each client to the timelock server - see the :ref:`separate migration docs <timelock-migration>`. For Cassandra KVS, this is automatic.

.. warning::

    Do not skip this step if your client uses DbKvs! Failure to migrate your client will cause **severe data corruption**, as Timelock will serve timestamps starting from 1.

14. Restart each client.

15. (Optional) To verify that the migration worked correctly, get a fresh timestamp for each client from the Timelock server.
    For each client, the timestamp returned should be strictly greater than the corresponding timestamp obtained in step 10.

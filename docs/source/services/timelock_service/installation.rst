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

8. Back up each client, following the steps declared in :ref:`backup-restore`.

9. Configure each client to use Timelock.
   Detailed documentation is :ref:`here <timelock-client-configuration>`.
   You must remove any ``leader``, ``timestamp``, or ``lock`` blocks; the ``timelock`` block to add looks like this:

.. code-block:: yaml

   atlasdb:
      timelock:
        client: tom
        serversList:
          servers:
            - palantir-1.com:8080
            - palantir-2.com:8080
            - palantir-3.com:8080
          sslConfiguration:
            trustStorePath: var/security/truststore.jks

10. Configure Timelock to respond to your clients.
    Add each client as a named entry in the ``clients`` block of your :ref:`timelock-server-configuration`.


.. code-block:: yaml

   clients:
      - tom
      - jerry

11. Restart your Timelock cluster. Once again, a healthcheck (see step 7) may be run to confirm that Timelock restarted correctly.

12. Restart each client.
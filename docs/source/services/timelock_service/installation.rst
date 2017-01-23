.. _timelock-installation:

Timelock Server Installation
============================

1. Clone the git repository.

   .. code:: bash

      git clone git@github.com:palantir/atlasdb.git; cd atlasdb

2. Generate a tar file with Gradle:

   .. code:: bash

      ./gradlew atlasdb-timelock-server:distTar

  This will place a tar file in the ``build/distributions`` directory of the ``atlasdb-timelock-server`` project. The
  tar file follows semantic versioning.

3. Copy this tar file to the server on which you want to deploy Timelock Server.
   (If you wish to run Timelock Server locally, this step may be omitted.)

4. On the server which you intend to deploy Timelock Server, untar the archive.

   .. code:: bash

      tar -zxvf atlasdb-timelock-server-0.25.0.sls.tgz; cd atlasdb-timelock-server-0.25.0

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

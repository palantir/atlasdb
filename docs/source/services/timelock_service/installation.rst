.. _installation:

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

   .. code:: bash

      scp atlasdb-timelock-server/build/distributions/atlasdb-timelock-server-0.25.0.sls.tgz <DESTINATION_HOST>

4. On the server which you intend to deploy Timelock Server, untar the archive.

   .. code:: bash

      tar -zxvf atlasdb-timelock-server-0.25.0.sls.tgz; cd atlasdb-timelock-server-0.25.0

5. Configure the Timelock Server, following the steps in configuration section (to be added).

   .. code:: bash

      vi var/conf/timelock.yml

6. Start the Timelock Server.

   .. code:: bash

      service/bin/init.sh start

   This should output the process ID of the Timelock Server. You can view the logs in the (by default) ``var/log``
   directory.

7. It may be useful to run a health-check to verify that the Timelock Server is running. To do this, you can curl
   the server and verify that it can give out timestamps. For example, if an allowed client of the Timelock Server is
   ``test``, then the following command should request a fresh timestamp.

   .. code:: bash

      curl -XPOST localhost:8080/test/timestamp/fresh-timestamp

   This should return an integer. Note that the command above will not work if you are using SSL. If you run the
   curl command again, you should obtain a strictly greater integer.

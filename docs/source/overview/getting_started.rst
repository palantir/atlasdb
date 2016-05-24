.. _getting-started:

===============
Getting Started
===============

.. _running-from-source:

Running from Source
===================

1. Clone the git repository:
   ``git clone git@github.com:palantir/atlasdb.git; cd atlasdb``.
2. Generate IDE configuration: ``./gradlew eclipse`` or
   ``./gradlew idea``.
3. Import projects into Eclipse/Intellij.

There are some examples of how to write queries in the `github examples
directory <https://github.com/palantir/atlasdb/tree/develop/examples>`__.

Running Tests Locally
=====================
Running atlas tests requires ``docker`` and ``docker-compose`` to be installed on the machine where they are running. Instructions for different operating systems can be found `here <https://docs.docker.com/engine/installation/>`__

Instructions for MacOS
----------------------
Mac users will need to install `docker-machine` and a virtualization environment as well to be able to run docker. Virtualbox is used in this example. Simplified set-up instructions are as follows:

1. Install ``docker-machine``, ``docker`` and ``docker-compose``
   
   .. code:: bash
   
      brew install docker docker-machine docker-compose

2. Set up a ``docker-machine`` for Virtualbox (named ``atlas`` in the example, name can be chosen freely by the user)

   .. code:: bash
   
      docker-machine create atlas --driver virtualbox

3. Set up the environment:
   
   .. code:: bash
   
      eval $(docker-machine env atlas)  
   
   These environment variables need to always be set before running the tests through `gradle`, or set manually as environment variables for the launch configurations in any IDE.

Depending on Published Artifacts
================================

AtlasDB is `hosted on
bintray <https://bintray.com/palantir/releases/atlasdb/view>`__. To
include in a gradle project:

1. Add bintray to your repository list:

   .. code:: javascript

       repositories {
           maven {
               url 'https://dl.bintray.com/palantir/releases/'
           }
       }

2. Add the atlasdb-client dependency to projects using AtlasDB:

   .. code:: javascript

       dependencies {
           compile 'com.palantir.atlasdb:atlasdb-client:0.3.3'
       }

Standalone JSON/REST Server
===========================

The standalone server is a lightweight way to try out AtlasDB and can be
done by running AtlasDbServer in the atlasdb-server project.

.. code:: bash

    AtlasDbServer server src/dist/atlasdb_standalone.yml

This is dropwizard service that runs all the needed parts and doesn't
force you to use the java client to get the benefits of atlas. See the
:ref:`atlas-server-api` for more details.

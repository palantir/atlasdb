.. _product-changes:

Developing a product to use the timelock service
================================================

All products deploying against the AtlasDB Timelock service should follow the following checklist.

1.  Ensure that the AtlasDB config contains :ref:`timelock-client-configuration`.
2. The `Jetty ALPN agent <https://github.com/jetty-project/jetty-alpn-agent#usage>`__ is added as a javaagent JVM argument.
   All AtlasDB clients will already have ``jetty-alpn-agent-2.0.6.jar`` in the classpath. This is required to establish HTTP/2 connections.

    .. code-block:: yaml

        java -javaagent:service/lib/jetty-alpn-agent-2.0.6.jar

3. Ensure that the timelock server has added the product as a client in the :ref:`timelock-server-clients` block.
   The client name should be same as the ``client`` field in the :ref:`timelock-client-configuration`.

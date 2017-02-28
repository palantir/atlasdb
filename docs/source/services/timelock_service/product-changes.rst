.. _product-changes:

Developing a product to use the timelock service
================================================

All products deploying against the AtlasDB Timelock service should adhere to the following checklist.

1. Ensure that the AtlasDB client config contains the ``timelock`` :ref:`config block <timelock-client-configuration>`.
2. The `Jetty ALPN agent <https://github.com/jetty-project/jetty-alpn-agent#usage>`__ is added as a javaagent JVM argument.
   All AtlasDB clients will already have ``jetty-alpn-agent-2.0.6.jar`` in the classpath. This is required to establish
   HTTP/2 connections, and failure to include this will result in falling back to HTTP/1.1 connections and see significant perf degradation.

    .. code-block:: yaml

        java -javaagent:service/lib/jetty-alpn-agent-2.0.6.jar

3. Ensure that the Timelock server has added the product as a client in its :ref:`client <timelock-server-clients>` block.
   The client name should be same as the ``client`` field in the :ref:`Timelock client configuration <timelock-client-configuration>`.
4. All users of the product should :ref:`migrate <timelock-migration>` from embedded timestamp/lock services to the timelock server post-upgrade.

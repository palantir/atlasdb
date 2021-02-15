.. _product-changes:

Developing a product to use the timelock service
================================================

All products deploying against the AtlasDB Timelock service should adhere to the following checklist.

1. Ensure that the AtlasDB client config contains the ``timelock`` :ref:`config block <timelock-client-configuration>`.
2. All users of the product should :ref:`migrate <timelock-migration>` from embedded timestamp/lock services to the timelock server post-upgrade.

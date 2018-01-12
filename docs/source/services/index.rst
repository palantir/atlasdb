========
Services
========

AtlasDB relies on three distinct services. They are the Key Value Service, the Lock Service, and the Timestamp Service.
Each of the services is described in detail below.  The intention of this documentation is to provide a high level
overview of the architecture of each of the services as well as the guarantees they uphold.

AtlasDB typically runs an embedded timestamp and lock service. However, we are currently developing an
external timestamp and lock service. Please consult the relevant section of the documentation for more detail.

.. toctree::
   :maxdepth: 2
   :titlesonly:

   key_value_services/index
   lock_service/index
   timestamp_service/index
   timelock_service/index
   qos_service/index

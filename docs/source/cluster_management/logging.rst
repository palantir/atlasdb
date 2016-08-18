.. _logging-configuration:

=====================
Logging Configuration
=====================

Profiling a Key Value Service
=============================

All Key Value Services created using ``TransactionManagers.create()`` will
be wrapped in a ``ProfilingKeyValueService``.  This service will log out timing
and metadata for requests made against the underlying KVS.  In order to activate
the logging simply enable ``TRACE`` logging for
``com.palantir.atlasdb.keyvalue.impl.ProfilingKeyValueService``.

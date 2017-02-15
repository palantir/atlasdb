.. _logging-configuration:

=====================
Logging Configuration
=====================

Profiling a Key Value Service
=============================

All Key Value Services created using ``TransactionManagers.create()`` will
be wrapped in a ``ProfilingKeyValueService``.  This service will log out timing
and metadata for requests made against the underlying KVS.  In order to activate
the logging, simply enable ``TRACE`` logging for
``com.palantir.atlasdb.keyvalue.impl.ProfilingKeyValueService``.

If you are using ``CassandraKeyValueService``, you can use additional tracing for deeper analysis.
For more information, see :ref:`enabling-cassandra-tracing`.

Debug Logging for Multiple Timestamp Services Error
===================================================

This logging is available from version 0.22.0.  It is intended for targeted use only 
(read: when you have consistent reproduction) as it is particularly verbose. 
Due to the volume, it is recommended that you send logging related to the timestamp service to a separate appender.
To do this, add the following to your logging configuration:

.. code:: yaml

    logging:
      loggers:
        com.palantir.timestamp:
          level: TRACE
          additive: false
          appenders:
            - archivedFileCount: 5
              archivedLogFilenamePattern: '{{service_home}}/var/log/timestamps-%d.log.gz'
              currentLogFilename: '{{service_home}}/var/log/timestamps.log'
              type: file

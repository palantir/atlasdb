.. _dropwizard-metrics:

===============
AtlasDB Metrics
===============

AtlasDB makes use of the Dropwizard `Metrics library <http://metrics.dropwizard.io/>`__ to
expose a global ``MetricRegistry`` called ``AtlasDbRegistry``. Users of AtlasDB should use ``AtlasDbMetrics.setMetricRegistry``
to inject their own ``MetricRegistry`` for their application prior to initializing the AtlasDB transaction manager.

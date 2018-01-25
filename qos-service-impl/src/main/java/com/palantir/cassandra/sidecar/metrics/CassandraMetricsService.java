/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.cassandra.sidecar.metrics;

import com.palantir.cassandra.sidecar.SharedSecretHeader;
import com.palantir.logsafe.Safe;
import java.util.Map;
import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

/**
 * Copied from internal sls Cassandra project.
 */
@Path("/metrics")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface CassandraMetricsService {
    /**
     * Get the value for a particular Cassandra metric. The possible metric combinations can be found in
     * http://cassandra.apache.org/doc/latest/operating/metrics.html.
     *
     * @param authHeader the shared secret
     * @param type The type of the Cassandra metric, mostly a sub-type of the metric name eg: CommitLog
     * @param name The name of the Cassandra metric eg: PendingTasks
     * @param attr The attribute you require for the metric eg: Value, p99 etc.
     * @param additionalParams Any other parameters to make the metric more specific eg: scope:keyspace
     * @return the metric object for the queried metric
     */
    @POST
    @Path("/{type}/{name}/{attribute}")
    Object getMetric(
            @HeaderParam(HttpHeaders.AUTHORIZATION) SharedSecretHeader authHeader,
            @Safe @PathParam(value = "type") String type,
            @Safe @PathParam(value = "name") String name,
            @Safe @PathParam(value = "attribute") String attr,
            @Safe Map<String, String> additionalParams);
}

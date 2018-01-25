/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.qos.metrics;

import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.palantir.logsafe.Safe;

/**
 * Copied from internal sls Cassandra project.
 */

@Path("/metrics")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface MetricsService {
    /**
     * Get the value for a particular Cassandra metric. The possible metric combinations can be found in
     * http://cassandra.apache.org/doc/latest/operating/metrics.html.
     *
     * @param type The type of the Cassandra metric, mostly a sub-type of the metric name eg: CommitLog
     * @param name The name of the Cassandra metric eg: PendingTasks
     * @param attr The attribute you require for the metric eg: Value, p99 etc.
     * @param additionalParams Any other parameters to make the metric more specific eg: scope:keyspace
     * @return the metric object for the queried metric
     */
    @POST
    @Path("/{type}/{name}/{attribute}")
    Object getMetric(
            @Safe @PathParam(value = "type") String type,
            @Safe @PathParam(value = "name") String name,
            @Safe @PathParam(value = "attribute") String attr,
            @Safe Map<String, String> additionalParams);
}

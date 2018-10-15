/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

@Path("/metrics")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface MetricsService {
    /**
     * Get the value for a particular metric.
     *
     * @param type The type of the metric, mostly a sub-type of the metric name eg: CommitLog
     * @param name The name of the metric eg: PendingTasks
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

/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.cassandra.sidecar.metrics;

import java.util.Map;

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
public interface CassandraMetricsService {
    /**
     * Backs up the SSTables for the specified keyspaces.
     * If no keyspaces are specified this operation succeeds, but does nothing.
     */
    @POST
    Object getMetric(
            @Safe @PathParam(value = "type") String type,
            @Safe @PathParam(value = "name") String name,
            @Safe @PathParam(value = "attribute") String attr,
            @Safe Map<String, String> additionalParams);
}

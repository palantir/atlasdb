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
package com.palantir.atlasdb.timestamp;

import com.palantir.logsafe.Safe;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

/**
 * ETE wrapper around a Timestamp service and a TimestampManagement service.
 */
@Path("ete-timestamp")
public interface EteTimestampResource {
    @POST // This has to be POST because we can't allow caching.
    @Path("fresh-timestamp")
    @Produces(MediaType.APPLICATION_JSON)
    long getFreshTimestamp();

    @POST
    @Path("fast-forward")
    @Produces(MediaType.APPLICATION_JSON)
    void fastForwardTimestamp(@Safe @QueryParam("currentTimestamp") long currentTimestamp);
}

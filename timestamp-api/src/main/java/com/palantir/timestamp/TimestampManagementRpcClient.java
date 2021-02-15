/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timestamp;

import com.palantir.logsafe.Safe;
import javax.annotation.CheckReturnValue;
import javax.annotation.meta.When;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path("{namespace}/timestamp-management")
public interface TimestampManagementRpcClient {
    @POST
    @Path("fast-forward")
    @Produces(MediaType.APPLICATION_JSON)
    void fastForwardTimestamp(
            @PathParam("namespace") String namespace, @Safe @QueryParam("currentTimestamp") long currentTimestamp);

    @GET
    @Path("ping")
    @Produces(MediaType.TEXT_PLAIN)
    @CheckReturnValue(when = When.NEVER)
    String ping(@PathParam("namespace") String namespace);
}

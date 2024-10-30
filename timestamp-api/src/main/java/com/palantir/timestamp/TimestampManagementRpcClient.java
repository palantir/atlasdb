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
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import javax.annotation.CheckReturnValue;
import javax.annotation.meta.When;

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

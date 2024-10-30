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

package com.palantir.lock.v2;

import com.palantir.processors.AutoDelegate;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/**
 * Interface describing timelock endpoints to be used by feign client factories to create raw clients.
 *
 * If you are adding a replacement for an endpoint, please version by number, e.g. a new version of
 * fresh-timestamp might be fresh-timestamp-2.
 */
@Path("/{namespace}/timelock")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@AutoDelegate
public interface TimelockRpcClient {

    @POST
    @Path("immutable-timestamp")
    long getImmutableTimestamp(@PathParam("namespace") String namespace);

    @POST
    @Path("current-time-millis")
    long currentTimeMillis(@PathParam("namespace") String namespace);
}

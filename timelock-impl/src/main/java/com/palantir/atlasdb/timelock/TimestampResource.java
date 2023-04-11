/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock;

import com.palantir.conjure.java.undertow.annotations.Handle;
import com.palantir.conjure.java.undertow.annotations.HttpMethod;
import com.palantir.logsafe.Safe;
import com.palantir.timestamp.TimestampRange;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path("/{namespace: (?!(tl|lw)/)[a-zA-Z0-9_-]+}")
public final class TimestampResource {
    private final TimelockNamespaces namespaces;

    public TimestampResource(TimelockNamespaces timelockNamespaces) {
        this.namespaces = timelockNamespaces;
    }

    @POST // This has to be POST because we can't allow caching.
    @Path("/timestamp/fresh-timestamp")
    @Produces(MediaType.APPLICATION_JSON)
    @Handle(method = HttpMethod.POST, path = "/{namespace}/timestamp/fresh-timestamp")
    public long getFreshTimestamp(@Safe @PathParam("namespace") @Handle.PathParam String namespace) {
        return namespaces.get(namespace).getTimestampService().getFreshTimestamp();
    }

    @POST // This has to be POST because we can't allow caching.
    @Path("/timestamp/fresh-timestamps")
    @Produces(MediaType.APPLICATION_JSON)
    @Handle(method = HttpMethod.POST, path = "/{namespace}/timestamp/fresh-timestamps")
    public TimestampRange getFreshTimestamps(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @QueryParam("number") @Handle.QueryParam(value = "number") int numTimestampsRequested) {
        return namespaces.get(namespace).getTimestampService().getFreshTimestamps(numTimestampsRequested);
    }
}

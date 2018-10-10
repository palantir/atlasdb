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
package com.palantir.atlasdb.qos;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.palantir.logsafe.Safe;

@Path("/")
public interface QosService {
    /**
     * Get the read limit for a client.
     * @param client the name of the client.
     * @return the number of bytes per second the client is allowed to read
     */
    @Path("{client: [a-zA-Z0-9_-]+}/read-limit")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    long readLimit(@Safe @PathParam("client") String client);

    /**
     * Get the write limit for a client.
     * @param client the name of the client.
     * @return the number of bytes per second the client is allowed to write
     */
    @Path("{client: [a-zA-Z0-9_-]+}/write-limit")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    long writeLimit(@Safe @PathParam("client") String client);
}

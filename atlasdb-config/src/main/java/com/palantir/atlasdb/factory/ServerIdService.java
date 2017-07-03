/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.atlasdb.factory;

import java.util.UUID;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/timestamp")
interface ServerIdService {
    @GET
    @Path("server-id")
    @Produces(MediaType.APPLICATION_JSON)
    UUID getServerId();
}

/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.atlasdb.factory;

import java.util.UUID;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

@Path("/timestamp")
interface ServerIdService {
    @GET
    @Path("server-id")
    UUID getServerId();
}

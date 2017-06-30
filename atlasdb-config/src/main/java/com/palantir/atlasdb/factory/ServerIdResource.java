/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.atlasdb.factory;

import java.util.UUID;

class ServerIdResource implements ServerIdService {

    private UUID serverId;

    ServerIdResource(UUID serverId) {
        this.serverId = serverId;
    }

    @Override
    public UUID getServerId() {
        return serverId;
    }
}

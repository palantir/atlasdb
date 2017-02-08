/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.atlasdb.timelock;

import java.net.URI;

import javax.ws.rs.NotSupportedException;

import com.palantir.atlasdb.timelock.paxos.WebsocketClientBase;
import com.palantir.atlasdb.timelock.paxos.WebsocketMessage;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;

public class TimestampWebsocketClient extends WebsocketClientBase implements TimestampService {

    public TimestampWebsocketClient(URI serverURI) {
        super(serverURI);
    }

    @Override
    public long getFreshTimestamp() {
        WebsocketMessage response = sendRequest("timestamp");
        return response.longAt(0);
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        throw new NotSupportedException();
    }

}
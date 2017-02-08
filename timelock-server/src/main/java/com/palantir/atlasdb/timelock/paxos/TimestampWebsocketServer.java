/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.atlasdb.timelock.paxos;

import org.java_websocket.WebSocket;

import com.palantir.timestamp.TimestampService;

import io.dropwizard.lifecycle.Managed;

public class TimestampWebsocketServer extends WebsocketServerBase implements Managed {

    private final TimestampService timestampService;

    public TimestampWebsocketServer(String uri, TimestampService timestampService) {
        super(uri);
        this.timestampService = timestampService;
    }


    @Override
    public WebsocketMessage processRequest(WebSocket conn, WebsocketMessage message) {
        if (message.methodName().equals("timestamp")) {
            Long timestamp = timestampService.getFreshTimestamp();
            return message.response(timestamp.toString());
        }

        throw new IllegalArgumentException("unknown method: " + message.methodName());
    }

}

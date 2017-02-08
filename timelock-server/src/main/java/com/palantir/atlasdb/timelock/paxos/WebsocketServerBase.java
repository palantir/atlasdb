/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.atlasdb.timelock.paxos;

import java.net.InetSocketAddress;
import java.net.URI;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dropwizard.lifecycle.Managed;


public abstract class WebsocketServerBase extends WebSocketServer implements Managed {

    private static final Logger log = LoggerFactory.getLogger(WebsocketServerBase.class);


    public WebsocketServerBase(String uriString) {
        this(URI.create(uriString));
    }

    public WebsocketServerBase(URI uri) {
        super(new InetSocketAddress(uri.getHost(), uri.getPort()));
        log.info("creating server at {}", uri);
    }
    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        log.info("websocket opened: {}", conn);
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        log.info("websocket closed: {}", conn);
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        log.error("websocket error: {}", conn, ex);
    }

    @Override
    public void onMessage(WebSocket conn, String data) {
        //log.info("recieved message {}", message);
        try {
            handleRequest(conn, data);
        } catch (Throwable e) {
            log.error("error handling message {}", data, e);
        }
    }

    private void handleRequest(WebSocket conn, String data) {
        WebsocketMessage message = WebsocketMessage.parse(data);

        WebsocketMessage reply = processRequest(conn, message);
        if (reply != null) {
            conn.send(reply.toString());
        }
    }

    public abstract WebsocketMessage processRequest(WebSocket conn, WebsocketMessage message);

}

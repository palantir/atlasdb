/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.atlasdb.timelock.paxos;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.java_websocket.WebSocket.READYSTATE;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import io.dropwizard.lifecycle.Managed;

/**
 *
 */
public abstract class WebsocketClientBase extends WebSocketClient implements Managed {

    private static final Logger log = LoggerFactory.getLogger(PaxosWebsocketClient.class);

    private final AtomicLong messageIdProvider = new AtomicLong(1);
    private final ConcurrentMap<String, CompletableFuture<WebsocketMessage>> sentMessages = Maps.newConcurrentMap();

    public WebsocketClientBase(URI serverURI) {
        super(serverURI);
    }

    @Override
    public void onOpen(ServerHandshake handshakedata) {
        log.info("websocket opened", this);
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        log.info("websocket closed", this);
    }

    @Override
    public void onError(Exception ex) {
        log.error("websocket encountered error", this, ex);
    }

    @Override
    public void onMessage(String data) {
        //log.info("recieved message {}", message);
        try {
            handleResponse(data);
        } catch (Throwable t) {
            log.error("error handling message {}", data, t);
        }
    }

    private void handleResponse(String data) {
        WebsocketMessage message = WebsocketMessage.parse(data);

        CompletableFuture<WebsocketMessage> future = sentMessages.get(message.messageId());
        if (future == null) {
            log.warn("recieved response for nonexistent messageId {}", message.messageId());
            return;
        }

        future.complete(message);
    }

    protected WebsocketMessage sendRequest(Object method, String...args) {
        waitUntilOpen();

        String messageId = Long.toString(messageIdProvider.getAndIncrement());
        String message = WebsocketMessage.request(messageId, method.toString(), args);

        CompletableFuture<WebsocketMessage> future = new CompletableFuture<>();
        sentMessages.put(messageId, future);
        send(message);

        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized void waitUntilOpen() {
        if (getReadyState() != READYSTATE.OPEN) {
            try {
                connectBlocking();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void start() throws Exception {
        log.info("starting");
        connect();
    }

    @Override
    public void stop() throws Exception {
        close();
    }

}

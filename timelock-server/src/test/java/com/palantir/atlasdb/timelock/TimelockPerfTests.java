/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.atlasdb.timelock;

import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.timestamp.TimestampService;

public class TimelockPerfTests {

    static final Logger log = LoggerFactory.getLogger(TimelockPerfTests.class);

    private static final String[] SERVER_URIS = new String[] {
            "http://localhost:8080/test",
            "http://localhost:8081/test",
            "http://localhost:8082/test"};

    private static final String[] WEBSOCKET_SERVER_URIS = new String[] {
            "http://localhost:8060",
            "http://localhost:8061",
            "http://localhost:8062"};

    private static final int NUM_REQUESTS = 10000;

    TimestampService httpTmestampService;
    TimestampService websocketTimestampService;

    @Before
    public void find_the_leader() {
        for (int i = 0; i < SERVER_URIS.length; i++) {
            if (isLeader(SERVER_URIS[i])) {
                httpTmestampService = createHttpProxy(SERVER_URIS[i]);
                websocketTimestampService = createWebsocketProxy(WEBSOCKET_SERVER_URIS[i]);
            }
        }

        if (httpTmestampService == null) {
            throw new IllegalStateException("could not successfully contact any server");
        }

        // sanity check
        httpTmestampService.getFreshTimestamp();
        websocketTimestampService.getFreshTimestamp();
    }

    @Test
    public void get_timestamp() {
        measureRequestTimes(() -> httpTmestampService.getFreshTimestamp(), "http");
        measureRequestTimes(() -> websocketTimestampService.getFreshTimestamp(), "websocket");
    }

    private void measureRequestTimes(Supplier<Long> timestampFunc, String name) {
        log.warn("starting test for {}", name);

        // warm up
        for (int i = 0; i < 1000; i++) {
            timestampFunc.get();
        }

        // time requests
        Stopwatch timer = Stopwatch.createStarted();

        long lastTs = -1;
        for (int i = 0; i < NUM_REQUESTS; i++) {
            long ts = timestampFunc.get();
            assertTrue(ts > lastTs);
            lastTs = ts;
        }

        long totalTime = timer.elapsed(TimeUnit.MILLISECONDS);
        log.warn("{}: {} requests", name, NUM_REQUESTS);
        log.warn("average request time: {} ms", totalTime / (double)NUM_REQUESTS);
        log.warn("total time: {} ms", totalTime);
    }

    private boolean isLeader(String serverUri) {
        try {
            createHttpProxy(serverUri).getFreshTimestamp();
            return true;
        } catch(Throwable e) {
            log.warn("error, trying another server: {}", e.getMessage());
            return false;
        }
    }

    private TimestampService createHttpProxy(String serverUri) {
        TimestampService timestampService = AtlasDbHttpClients.createProxy(
                Optional.absent(),
                serverUri,
                TimestampService.class);
        return timestampService;
    }

    private TimestampService createWebsocketProxy(String serverUri) {
        return new TimestampWebsocketClient(URI.create(serverUri));
    }


}

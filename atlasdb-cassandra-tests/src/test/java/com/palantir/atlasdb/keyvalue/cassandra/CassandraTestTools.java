package com.palantir.atlasdb.keyvalue.cassandra;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.joda.time.Duration;

import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.core.ConditionTimeoutException;

/**
 * Utilities for ETE tests
 * Created by aloro on 12/04/2016.
 */
class CassandraTestTools {
    private CassandraTestTools() {
        // Empty constructor for utility class
    }

    static void waitTillServiceIsUp(String host, int port, Duration timeout) {
        try {
            Awaitility.await()
                    .pollInterval(50, TimeUnit.MILLISECONDS)
                    .atMost(timeout.getMillis(), TimeUnit.MILLISECONDS)
                    .until(isPortListening(host, port));
        } catch (ConditionTimeoutException e) {
            throw new IllegalStateException("Timeout for port " + port + " on host " + host + ".");
        }
    }

    private static Callable<Boolean> isPortListening(String host, int port) {
        return () -> {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), 500);
                return true;
            } catch (IOException e) {
                return false;
            }
        };
    }
}

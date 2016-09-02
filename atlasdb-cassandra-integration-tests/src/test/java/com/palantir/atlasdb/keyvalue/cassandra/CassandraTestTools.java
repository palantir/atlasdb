/**
 * Copyright 2015 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.cassandra;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
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

    static Future async(ExecutorService executorService, Runnable callable) {
        return executorService.submit(callable);
    }

    static void assertThatFutureDidNotSucceedYet(Future future) throws InterruptedException {
        if (future.isDone()) {
            try {
                future.get();
                throw new AssertionError("Future task should have failed but finished successfully");
            } catch (ExecutionException e) {
                // if execution is done, we expect it to have failed
            }
        }
    }
}

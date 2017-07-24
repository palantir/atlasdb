/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.timelock.clock;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.net.ssl.SSLSocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.logsafe.SafeArg;

/**
 * ClockSkewMonitor keeps track of the system time of the other nodes in the cluster, and compares it to the local
 * clock. It's purpose is to monitor if the other nodes' clock progress at the same pace as the local clock.
 */
public final class ClockSkewMonitor {

    private static final long SLEEP_TIME_SECONDS = 1; // 1 s
    private static final long NANOS_PER_SECOND = 1_000_000_000; // 10^9
    private static final long MAX_TIME_SINCE_PREVIOUS_REQUEST_NANOS = 10 * SLEEP_TIME_SECONDS * NANOS_PER_SECOND; // 10s
    private static final long MAX_REQUEST_TIME_NANOS = 10_000_000; // 10 ms
    private static final long WARN_SKEW_THRESHOLD_NANOS = 10_000_000; // 10 ms
    private static final long ERROR_SKEW_THRESHOLD_NANOS = 50_000_000; // 50 ms

    private static final MetricRegistry METRIC_REGISTRY = AtlasDbMetrics.getMetricRegistry();

    private final Logger log = LoggerFactory.getLogger(ClockSkewMonitor.class);
    private final Map<String, ClockService> monitorsByServer;
    private final Map<String, RequestTime> previousRequestsByServer;
    private final Supplier<Boolean> shouldRunClockSkewMonitor;

    public static ClockSkewMonitor create(Set<String> remoteServers, Optional<SSLSocketFactory> optionalSecurity,
            Supplier<Boolean> shouldRunClockSkewMonitor) {
        Map<String, ClockService> monitors = Maps.toMap(remoteServers,
                (remoteServer) -> AtlasDbHttpClients.createProxy(optionalSecurity, remoteServer, ClockService.class));

        Map<String, RequestTime> previousRequests = Maps.newHashMap();
        for (String server : remoteServers) {
            previousRequests.put(server, RequestTime.EMPTY);
        }

        return new ClockSkewMonitor(monitors, previousRequests, shouldRunClockSkewMonitor);
    }

    private ClockSkewMonitor(
            Map<String, ClockService> monitorsByServer,
            Map<String, RequestTime> previousRequestsByServer,
            Supplier<Boolean> shouldRunClockSkewMonitor) {
        this.monitorsByServer = monitorsByServer;
        this.previousRequestsByServer = previousRequestsByServer;
        this.shouldRunClockSkewMonitor = shouldRunClockSkewMonitor;
    }

    public void run() {
        Executors.newSingleThreadScheduledExecutor().schedule(
                () -> {
                    try {
                        if (shouldRunClockSkewMonitor.get()) {
                            runInternal();
                        }
                    } catch (Throwable t) {
                        log.warn("ClockSkewMonitor threw an exception", t);
                    }
                }, SLEEP_TIME_SECONDS, TimeUnit.SECONDS
        );
    }

    private void runInternal() {
        monitorsByServer.forEach((server, monitor) -> {
            long localTimeAtStart = System.nanoTime();
            long remoteSystemTime = monitor.getSystemTimeInNanos();
            long localTimeAtEnd = System.nanoTime();

            RequestTime previousRequest = previousRequestsByServer.get(server);
            RequestTime newRequest = new RequestTime(localTimeAtStart, localTimeAtEnd, remoteSystemTime);
            compareClockSkew(server, newRequest, previousRequest);
        });
    }

    private void compareClockSkew(String server, RequestTime newRequest, RequestTime previousRequest) {
        if (previousRequest.equals(RequestTime.EMPTY)) {
            previousRequestsByServer.put(server, newRequest);
            return;
        }

        long maxElapsedTime = newRequest.localTimeAtEnd - previousRequest.localTimeAtStart;
        long minElapsedTime = newRequest.localTimeAtStart - previousRequest.localTimeAtEnd;
        long remoteElapsedTime = newRequest.remoteSystemTime - previousRequest.remoteSystemTime;

        if (minElapsedTime > MAX_TIME_SINCE_PREVIOUS_REQUEST_NANOS) {
            log.debug("It's been a long time since we last queried the server."
                            + " Ignoring the skew, since it's not representative.",
                    SafeArg.of("remoteElapsedTime", remoteElapsedTime));
            previousRequestsByServer.put(server, newRequest);
            return;
        }

        // maxElapsedTime - minElapsedTime = time for previous request and current request to complete.
        if (maxElapsedTime - minElapsedTime > 2 * MAX_REQUEST_TIME_NANOS) {
            log.debug("A request took too long to complete."
                            + " Ignoring the skew, since it's not representative.",
                    SafeArg.of("requestsDuration", maxElapsedTime - minElapsedTime));
            previousRequestsByServer.put(server, newRequest);
            return;
        }

        if (remoteElapsedTime < minElapsedTime || remoteElapsedTime > maxElapsedTime) {
            long skew;

            if (remoteElapsedTime < minElapsedTime) {
                skew = minElapsedTime - remoteElapsedTime;
            } else {
                skew = maxElapsedTime - remoteElapsedTime;
            }

            if (skew > WARN_SKEW_THRESHOLD_NANOS && skew < ERROR_SKEW_THRESHOLD_NANOS) {
                log.warn("Skew (in nanos) greater than expected", skew);
            } else if (skew >= ERROR_SKEW_THRESHOLD_NANOS) {
                log.error("Skew (in nanos) much greater than expected", skew);
            }

            METRIC_REGISTRY.histogram(String.format("clock-skew-%s-histogram", server))
                    .update(skew);
            METRIC_REGISTRY.counter(String.format("clock-skew-%s-counter", server)).inc();
        }

        previousRequestsByServer.put(server, newRequest);

        METRIC_REGISTRY.histogram(String.format("clock-pace-%s-local-min", server))
                .update(minElapsedTime);
        METRIC_REGISTRY.histogram(String.format("clock-pace-%s-local-max", server))
                .update(maxElapsedTime);
        METRIC_REGISTRY.histogram(String.format("clock-pace-%s-remote", server))
                .update(remoteElapsedTime);
    }

    private static class RequestTime {
        // Since we expect localTimeAtStart != localTimeAtEnd, it's safe to have an empty request time.
        private static final RequestTime EMPTY = new RequestTime(0L, 0L, 0L);

        private final long localTimeAtStart;
        private final long localTimeAtEnd;
        private final long remoteSystemTime;

        RequestTime(long localTimeAtStart, long localTimeAtEnd, long remoteSystemTime) {
            this.localTimeAtStart = localTimeAtStart;
            this.localTimeAtEnd = localTimeAtEnd;
            this.remoteSystemTime = remoteSystemTime;
        }
    }
}

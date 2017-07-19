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

import static java.lang.Thread.sleep;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
public final class ClockSkewMonitor implements Runnable {

    private static final long sleepTimeMillis = 1_000; // 1 s
    private static final long millisToNanos = 1_000_000; // 10^6
    private static final long maxTimeSincePreviousRequestNanos = 10 * sleepTimeMillis * millisToNanos; // 10 s
    private static final long maxRequestTimeNanos = 10_000_000; // 10 ms

    private static final MetricRegistry metricRegistry = AtlasDbMetrics.getMetricRegistry();

    private final Logger logger = LoggerFactory.getLogger(ClockSkewMonitor.class);
    private final Map<String, ClockService> monitors;
    private final Map<String, RequestTime> previousRequests;
    private final Supplier<Boolean> shouldRunClockSkewMonitor;

    public static ClockSkewMonitor create(Set<String> remoteServers, Optional<SSLSocketFactory> optionalSecurity,
            Supplier<Boolean> shouldRunClockSkewMonitor) {
        Map<String, ClockService> monitors = Maps.toMap(remoteServers,
                (remoteServer) -> AtlasDbHttpClients.createProxy(optionalSecurity, remoteServer, ClockService.class));
        Map<String, RequestTime> previousRequests = Maps.toMap(remoteServers, (remoteServer) -> null);

        return new ClockSkewMonitor(monitors, previousRequests, shouldRunClockSkewMonitor);
    }

    private ClockSkewMonitor(
            Map<String, ClockService> monitors,
            Map<String, RequestTime> previousRequests,
            Supplier<Boolean> shouldRunClockSkewMonitor) {
        this.monitors = monitors;
        this.previousRequests = previousRequests;
        this.shouldRunClockSkewMonitor = shouldRunClockSkewMonitor;
    }

    @Override
    public void run() {
        while (true) {
            try {
                sleep(sleepTimeMillis);
                if (shouldRunClockSkewMonitor.get()) {
                    runInternal();
                }
            } catch (InterruptedException e) {
                logger.warn("The clockSkewMonitor thread was interrupted. Please restart the service to restart"
                        + " the clock");
            }
        }
    }

    private void runInternal() {
        for (Map.Entry<String, ClockService> entry : monitors.entrySet()) {
            String server = entry.getKey();
            ClockService monitor = entry.getValue();

            long localTimeAtStart = System.nanoTime();
            long remoteSystemTime = monitor.getSystemTimeInNanos();
            long localTimeAtEnd = System.nanoTime();

            RequestTime previousRequest = previousRequests.get(server);
            RequestTime newRequest = new RequestTime(localTimeAtStart, localTimeAtEnd, remoteSystemTime);
            compareClockSkew(server, newRequest, previousRequest);
        }
    }

    private void compareClockSkew(String server, RequestTime newRequest, RequestTime previousRequest) {
        if (previousRequest == null) {
            previousRequests.put(server, newRequest);
            return;
        }

        long maxSkew = newRequest.localTimeAtEnd - previousRequest.localTimeAtStart;
        long minSkew = newRequest.localTimeAtStart - previousRequest.localTimeAtEnd;
        long remoteSkew = newRequest.remoteSystemTime - previousRequest.remoteSystemTime;

        if (minSkew > maxTimeSincePreviousRequestNanos) {
            logger.debug("It's been a long time since we last queried the server."
                            + " Ignoring the skew, since it's not representative.",
                    SafeArg.of("remoteSkew", remoteSkew));
            previousRequests.put(server, newRequest);
            return;
        }

        // maxSkew - minSkew = time for previous request and current request to complete.
        if (maxSkew - minSkew > 2 * maxRequestTimeNanos) {
            logger.debug("A request took too long to complete."
                            + " Ignoring the skew, since it's not representative.",
                    SafeArg.of("requestsDuration", maxSkew - minSkew));
            previousRequests.put(server, newRequest);
            return;
        }

        if (remoteSkew < minSkew) {
            logger.info("Remote clock pace lower than expected",
                    SafeArg.of("remoteSkew", remoteSkew), SafeArg.of("minSkew", minSkew));

            metricRegistry.histogram(String.format("clock-skew-%s-below-histogram", server))
                    .update(minSkew - remoteSkew);
            metricRegistry.counter(String.format("clock-skew-%s-below-counter", server)).inc();
        }

        if (maxSkew > remoteSkew) {
            logger.info("Remote clock pace greater than expected",
                    SafeArg.of("remoteSkew", remoteSkew), SafeArg.of("maxSkew", maxSkew));

            metricRegistry.histogram(String.format("clock-skew-%s-above-histogram", server))
                    .update(maxSkew - remoteSkew);
            metricRegistry.counter(String.format("clock-skew-%s-above-counter", server)).inc();
        }

        previousRequests.put(server, newRequest);

        metricRegistry.histogram(String.format("clock-pace-%s-local-min", server))
                .update(minSkew);
        metricRegistry.histogram(String.format("clock-pace-%s-local-max", server))
                .update(maxSkew);
        metricRegistry.histogram(String.format("clock-pace-%s-remote", server))
                .update(remoteSkew);
    }

    private class RequestTime {
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

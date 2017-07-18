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

import javax.net.ssl.SSLSocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.logsafe.SafeArg;

public class ClockSkewMonitor implements Runnable {

    private static long sleepTimeMillis = 1_000;
    private static long millisToNanos = 1_000_000;
    private static long maxRequestTimeNanos = 10_000_000;
    private static long maxInterRequestTimeNanos = 10 * sleepTimeMillis * millisToNanos;

    private Logger logger = LoggerFactory.getLogger(ClockSkewMonitor.class);
    private Map<String, ClockService> monitors;
    private Map<String, RequestTime> previousRequests;

    public ClockSkewMonitor(Set<String> remoteServers, Optional<SSLSocketFactory> optionalSecurity) {
        monitors = Maps.toMap(remoteServers,
                (remoteServer) -> AtlasDbHttpClients.createProxy(optionalSecurity, remoteServer, ClockService.class));
        previousRequests = Maps.toMap(remoteServers, (remoteServer) -> null);
    }

    @Override
    public void run() {
        while (true) {
            try {
                sleep(sleepTimeMillis);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

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
    }

    private void compareClockSkew(String server, RequestTime newRequest, RequestTime previousRequest) {
        if (previousRequest == null) {
            previousRequests.put(server, newRequest);
            return;
        }

        long maxSkew = newRequest.localTimeAtEnd - previousRequest.localTimeAtStart;
        long minSkew = newRequest.localTimeAtStart - previousRequest.localTimeAtEnd;
        long remoteSkew = newRequest.remoteSystemTime - previousRequest.remoteSystemTime;

        if (minSkew > maxInterRequestTimeNanos) {
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
            logger.debug("Remote clock skew smaller than allowed",
                    SafeArg.of("remoteSkew", remoteSkew), SafeArg.of("minSkew", minSkew));
        }

        if (maxSkew > remoteSkew) {
            logger.debug("Remote clock skew greater than allowed",
                    SafeArg.of("remoteSkew", remoteSkew), SafeArg.of("maxSkew", maxSkew));
        }

        previousRequests.put(server, newRequest);
        // add to remoteSkew to some metric, along with minSkew and maxSkew.
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

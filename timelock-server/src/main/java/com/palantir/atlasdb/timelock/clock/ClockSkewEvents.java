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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.palantir.logsafe.SafeArg;

public class ClockSkewEvents {
    private static final long WARN_SKEW_THRESHOLD_NANOS = 10_000_000; // 10 ms
    private static final long ERROR_SKEW_THRESHOLD_NANOS = 50_000_000; // 50 ms

    private final Logger log = LoggerFactory.getLogger(ClockSkewEvents.class);
    private final MetricRegistry metricRegistry;

    public ClockSkewEvents(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    public void tooMuchTimeSincePreviousRequest(long remoteElapsedTime) {
        log.debug("It's been a long time since we last queried the server."
                        + " Ignoring the skew, since it's not representative.",
                SafeArg.of("remoteElapsedTime", remoteElapsedTime));
    }

    public void requestsTookTooLong(long minElapsedTime, long maxElapsedTime) {
        log.debug("A request took too long to complete."
                        + " Ignoring the skew, since it's not representative.",
                SafeArg.of("requestsDuration", maxElapsedTime - minElapsedTime));
    }

    public void clockSkew(String server, long skew) {
        if (skew > WARN_SKEW_THRESHOLD_NANOS && skew < ERROR_SKEW_THRESHOLD_NANOS) {
            log.warn("Skew (in nanos) greater than expected", skew);
        } else if (skew >= ERROR_SKEW_THRESHOLD_NANOS) {
            log.error("Skew (in nanos) much greater than expected", skew);
        }

        metricRegistry.histogram(String.format("clock-skew-%s", server))
                .update(skew);
    }

    public void requestPace(String server, long minElapsedTime, long maxElapsedTime, long remoteElapsedTime) {
        metricRegistry.histogram(String.format("clock-pace-%s-local-min", server))
                .update(minElapsedTime);
        metricRegistry.histogram(String.format("clock-pace-%s-local-max", server))
                .update(maxElapsedTime);
        metricRegistry.histogram(String.format("clock-pace-%s-remote", server))
                .update(remoteElapsedTime);
    }

    public void exception(Throwable t) {
        metricRegistry.counter("clock-monitor-exception-counter").inc();
        log.warn("ClockSkewMonitor threw an exception", t);
    }
}

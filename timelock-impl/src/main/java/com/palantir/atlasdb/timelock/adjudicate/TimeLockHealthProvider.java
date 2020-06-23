/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.timelock.adjudicate;

import java.util.Collection;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;

public class TimeLockHealthProvider {
    private final ScheduledExecutorService executor = PTExecutors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory("TimeLockHealthProvider", true));

    private static final Logger log = LoggerFactory.getLogger(TimeLockHealthProvider.class);

    public static TimeLockHealthProvider create() {
        TimeLockHealthProvider timeLockHealthProvider = new TimeLockHealthProvider();
        timeLockHealthProvider.scheduleWithFixedDelay();
        return timeLockHealthProvider;
    }

    private void scheduleWithFixedDelay() {
        executor.scheduleWithFixedDelay(() -> {
                    try {
                        pointEstimateTimeLockHealth();
                    } catch (Exception e) {
                        log.warn("Failed to analyze metrics while adjudicating TimeLock.", e);
                    }
                },
                90,
                30,
                TimeUnit.SECONDS);
    }

    private HealthStatus pointEstimateTimeLockHealth() {
        Collection<ServiceHealthTracker.Service> services = FeedbackReportsSink.getTrackedServices();
        int targetUnhealthyServices = services.size() / 3;
        services = services.stream().filter(
                service -> !service.nodes().isEmpty()).collect(Collectors.toList());

        return services
                .stream()
                .filter(service -> ServiceHealthTracker.getHealthStatus(service) == HealthStatus.UNHEALTHY).count()
                >= targetUnhealthyServices ? HealthStatus.UNHEALTHY : HealthStatus.HEALTHY;
    }

    public void close() {
        executor.shutdown();
    }
}

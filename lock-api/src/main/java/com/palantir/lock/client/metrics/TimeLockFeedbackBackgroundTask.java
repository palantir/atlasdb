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

package com.palantir.lock.client.metrics;


import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.timelock.adjudicate.feedback.TimeLockClientFeedbackService;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.client.ConjureTimelockServiceBlockingMetrics;
import com.palantir.logsafe.SafeArg;
import com.palantir.timelock.feedback.ConjureTimeLockClientFeedback;
import com.palantir.timelock.feedback.EndpointStatistics;
import com.palantir.tokens.auth.AuthHeader;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class TimeLockFeedbackBackgroundTask implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(
            TimeLockFeedbackBackgroundTask.class);

    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer omitted");
    private static final String TIMELOCK_FEEDBACK_THREAD_PREFIX = "TimeLockFeedbackBackgroundTask";
    private static final Duration timeLockClientFeedbackReportInterval = Duration.ofSeconds(30);

    private final ScheduledExecutorService executor = PTExecutors.newSingleThreadScheduledExecutor(
                    new NamedThreadFactory(TIMELOCK_FEEDBACK_THREAD_PREFIX, true));

    private final UUID nodeId = UUID.randomUUID();
    private ConjureTimelockServiceBlockingMetrics conjureTimelockServiceBlockingMetrics;
    private Supplier<String> versionSupplier;
    private String serviceName;
    Supplier<List<TimeLockClientFeedbackService>> timeLockClientFeedbackServices;

    private TimeLockFeedbackBackgroundTask(TaggedMetricRegistry taggedMetricRegistry, Supplier<String> versionSupplier,
            String serviceName,
            Supplier<List<TimeLockClientFeedbackService>> timeLockClientFeedbackServices) {
        this.conjureTimelockServiceBlockingMetrics = ConjureTimelockServiceBlockingMetrics.of(taggedMetricRegistry);
        this.versionSupplier = versionSupplier;
        this.serviceName = serviceName;
        this.timeLockClientFeedbackServices = timeLockClientFeedbackServices;
    }

    public static TimeLockFeedbackBackgroundTask create(TaggedMetricRegistry taggedMetricRegistry,
            Supplier<String> versionSupplier,
            String serviceName,
            Supplier<List<TimeLockClientFeedbackService>> timeLockClientFeedbackServices) {
        TimeLockFeedbackBackgroundTask task = new TimeLockFeedbackBackgroundTask(taggedMetricRegistry,
                versionSupplier, serviceName, timeLockClientFeedbackServices);
        task.scheduleWithFixedDelay();
        return task;
    }


    public void scheduleWithFixedDelay() {
        executor.scheduleWithFixedDelay(() -> {
            try {
                ConjureTimeLockClientFeedback feedbackReport = ConjureTimeLockClientFeedback
                        .builder()
                        .startTransaction(getEndpointStatsForStartTxn())
                        .leaderTime(getEndpointStatsForLeaderTime())
                        .atlasVersion(versionSupplier.get())
                        .nodeId(nodeId)
                        .serviceName(serviceName)
                        .build();
                timeLockClientFeedbackServices
                        .get()
                        .forEach(service -> reportClientFeedbackToService(feedbackReport, service));
                log.info("The TimeLock client metrics for startTransaction endpoint aggregated "
                                + "over the last 1 minute - {}",
                        SafeArg.of("startTxnStats", feedbackReport));
            } catch (Exception e) {
                log.warn("A problem occurred while reporting client feedback for timeLock adjudication.", e);
            }
        },
                timeLockClientFeedbackReportInterval.getSeconds(),
                timeLockClientFeedbackReportInterval.getSeconds(),
                TimeUnit.SECONDS);
    }

    private void reportClientFeedbackToService(ConjureTimeLockClientFeedback feedbackReport,
            TimeLockClientFeedbackService service) {
        try {
            service.reportFeedback(AUTH_HEADER, feedbackReport);
        } catch (Exception e) {
            // we do not want this exception to bubble up so that feedback can be reported to other hosts
            log.warn("Failed to report feedback to TimeLock host.", e);
        }
    }

    private EndpointStatistics getEndpointStatsForLeaderTime() {
        return EndpointStatistics.of(getP99ForLeaderTime(),
                getOneMinuteRateForLeaderTime());
    }

    private double getOneMinuteRateForLeaderTime() {
        return conjureTimelockServiceBlockingMetrics.leaderTime().getOneMinuteRate();
    }

    private double getP99ForLeaderTime() {
        return conjureTimelockServiceBlockingMetrics
                .leaderTime()
                .getSnapshot()
                .get99thPercentile();
    }

    private EndpointStatistics getEndpointStatsForStartTxn() {
        return EndpointStatistics.of(getP99ForStartTxn(),
                getOneMinuteRateForStartTxn());
    }

    private double getOneMinuteRateForStartTxn() {
        return conjureTimelockServiceBlockingMetrics.startTransactions().getOneMinuteRate();
    }

    private double getP99ForStartTxn() {
        return conjureTimelockServiceBlockingMetrics
                .startTransactions()
                .getSnapshot()
                .get99thPercentile();
    }

    @Override
    public void close() {
        executor.shutdown();
    }
}

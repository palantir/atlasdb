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

import com.codahale.metrics.Timer;
import com.palantir.atlasdb.timelock.adjudicate.feedback.TimeLockClientFeedbackService;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.client.ConjureTimelockServiceBlockingMetrics;
import com.palantir.refreshable.Refreshable;
import com.palantir.timelock.feedback.ConjureTimeLockClientFeedback;
import com.palantir.timelock.feedback.EndpointStatistics;
import com.palantir.tokens.auth.AuthHeader;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class TimeLockFeedbackBackgroundTask implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(
            TimeLockFeedbackBackgroundTask.class);

    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer omitted");
    private static final String TIMELOCK_FEEDBACK_THREAD_PREFIX = "TimeLockFeedbackBackgroundTask";
    private static final Duration TIMELOCK_CLIENT_FEEDBACK_REPORT_INTERVAL = Duration.ofSeconds(30);

    private final ScheduledExecutorService executor = PTExecutors.newSingleThreadScheduledExecutor(
                    new NamedThreadFactory(TIMELOCK_FEEDBACK_THREAD_PREFIX, true));

    private final UUID nodeId = UUID.randomUUID();
    private ConjureTimelockServiceBlockingMetrics conjureTimelockServiceBlockingMetrics;
    private Supplier<String> versionSupplier;
    private String serviceName;
    private String namespace;
    private Refreshable<List<TimeLockClientFeedbackService>> timeLockClientFeedbackServices;

    private TimeLockFeedbackBackgroundTask(TaggedMetricRegistry taggedMetricRegistry,
            Supplier<String> versionSupplier,
            String serviceName,
            Refreshable<List<TimeLockClientFeedbackService>> timeLockClientFeedbackServices,
            String namespace) {
        this.conjureTimelockServiceBlockingMetrics = ConjureTimelockServiceBlockingMetrics.of(taggedMetricRegistry);
        this.versionSupplier = versionSupplier;
        this.serviceName = serviceName;
        this.timeLockClientFeedbackServices = timeLockClientFeedbackServices;
        this.namespace = namespace;
    }

    public static TimeLockFeedbackBackgroundTask create(TaggedMetricRegistry taggedMetricRegistry,
            Supplier<String> versionSupplier,
            String serviceName,
            Refreshable<List<TimeLockClientFeedbackService>> timeLockClientFeedbackServices,
            String namespace) {
        TimeLockFeedbackBackgroundTask task = new TimeLockFeedbackBackgroundTask(taggedMetricRegistry,
                versionSupplier,
                serviceName,
                timeLockClientFeedbackServices,
                namespace);
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
                        .namespace(namespace)
                        .build();
                timeLockClientFeedbackServices
                        .current()
                        .forEach(service -> reportClientFeedbackToService(feedbackReport, service));
            } catch (Exception e) {
                log.warn("A problem occurred while reporting client feedback for timeLock adjudication.", e);
            }
        },
                TIMELOCK_CLIENT_FEEDBACK_REPORT_INTERVAL.getSeconds(),
                TIMELOCK_CLIENT_FEEDBACK_REPORT_INTERVAL.getSeconds(),
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
        return EndpointStatistics
                .builder()
                .p99(getP99ForLeaderTime())
                .oneMin(getOneMinuteRateForLeaderTime())
                .errorRate(getErrorRateForLeaderTime())
                .build();
    }

    private double getErrorRateForLeaderTime() {
        return conjureTimelockServiceBlockingMetrics.leaderTimeErrors().getOneMinuteRate();
    }

    private double getOneMinuteRateForLeaderTime() {
        return conjureTimelockServiceBlockingMetrics.leaderTime().getOneMinuteRate();
    }

    private double getP99ForLeaderTime() {
        return getP99(() -> conjureTimelockServiceBlockingMetrics.leaderTime());

    }

    private EndpointStatistics getEndpointStatsForStartTxn() {
        return EndpointStatistics
                .builder()
                .p99(getP99ForStartTxn())
                .oneMin(getOneMinuteRateForStartTxn())
                .errorRate(getErrorRateForStartTxn())
                .build();
    }

    private double getErrorRateForStartTxn() {
        return conjureTimelockServiceBlockingMetrics.startTransactionErrors().getOneMinuteRate();
    }

    private double getOneMinuteRateForStartTxn() {
        return conjureTimelockServiceBlockingMetrics.startTransactions().getOneMinuteRate();
    }

    private double getP99ForStartTxn() {
        return getP99(() -> conjureTimelockServiceBlockingMetrics.startTransactions());
    }

    private double getP99(Supplier<Timer> timerSupplier) {
        return timerSupplier.get()
                .getSnapshot()
                .get99thPercentile();
    }

    @Override
    public void close() {
        executor.shutdown();
    }
}

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

package com.palantir.timelock.corruption.detection;

import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.SafeArg;
import com.palantir.timelock.corruption.TimeLockCorruptionNotifier;
import com.palantir.timelock.corruption.handle.LocalCorruptionHandler;
import com.palantir.timelock.history.PaxosLogHistoryProvider;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LocalCorruptionDetector implements CorruptionDetector {
    private static final Logger log = LoggerFactory.getLogger(LocalCorruptionDetector.class);

    private static final Duration DEFAULT_TIMELOCK_CORRUPTION_ANALYSIS_INTERVAL = Duration.ofMinutes(5);
    private static final String CORRUPTION_DETECTOR_THREAD_PREFIX = "timelock-corruption-detector";

    private final ScheduledExecutorService executor = PTExecutors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory(CORRUPTION_DETECTOR_THREAD_PREFIX, true));
    private final LocalCorruptionHandler corruptionHandler;
    private final PaxosLogHistoryProvider historyProvider;
    private final Duration timelockCorruptionAnalysisInterval;

    private volatile CorruptionStatus localCorruptionState = CorruptionStatus.HEALTHY;
    private volatile CorruptionHealthReport localCorruptionReport = CorruptionHealthReport.defaultHealthyReport();

    public static LocalCorruptionDetector create(
            PaxosLogHistoryProvider historyProvider,
            List<TimeLockCorruptionNotifier> corruptionNotifiers,
            Optional<Long> timelockCorruptionAnalysisIntervalSeconds) {
        LocalCorruptionDetector localCorruptionDetector = new LocalCorruptionDetector(
                historyProvider, corruptionNotifiers, timelockCorruptionAnalysisIntervalSeconds);

        localCorruptionDetector.scheduleWithFixedDelay();
        return localCorruptionDetector;
    }

    private LocalCorruptionDetector(
            PaxosLogHistoryProvider historyProvider,
            List<TimeLockCorruptionNotifier> corruptionNotifiers,
            Optional<Long> timelockCorruptionAnalysisIntervalSeconds) {
        this.historyProvider = historyProvider;
        this.corruptionHandler = new LocalCorruptionHandler(corruptionNotifiers);
        this.timelockCorruptionAnalysisInterval = timelockCorruptionAnalysisIntervalSeconds
                .map(Duration::ofSeconds)
                .orElse(DEFAULT_TIMELOCK_CORRUPTION_ANALYSIS_INTERVAL);
    }

    private void scheduleWithFixedDelay() {
        log.info("Scheduling corruption checks to run every {} seconds", SafeArg.of("interval",
                timelockCorruptionAnalysisInterval.getSeconds()));
        executor.scheduleWithFixedDelay(
                () -> {
                    localCorruptionReport = analyzeHistoryAndBuildCorruptionHealthReport();
                    processLocalHealthReport();
                },
                timelockCorruptionAnalysisInterval.getSeconds(),
                timelockCorruptionAnalysisInterval.getSeconds(),
                TimeUnit.SECONDS);
    }

    private CorruptionHealthReport analyzeHistoryAndBuildCorruptionHealthReport() {
        return HistoryAnalyzer.corruptionHealthReportForHistory(historyProvider.getHistory());
    }

    private void processLocalHealthReport() {
        localCorruptionState = getLocalCorruptionState(localCorruptionReport);
        if (localCorruptionState.shouldRejectRequests()) {
            corruptionHandler.notifyRemoteServersOfCorruption();
        }
    }

    CorruptionStatus getLocalCorruptionState(CorruptionHealthReport latestReport) {
        return latestReport.shouldRejectRequests()
                ? CorruptionStatus.DEFINITIVE_CORRUPTION_DETECTED_BY_LOCAL
                : localCorruptionState;
    }

    public CorruptionHealthReport corruptionHealthReport() {
        return localCorruptionReport;
    }

    @Override
    public boolean shouldRejectRequests() {
        return localCorruptionState.shouldRejectRequests();
    }
}

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
import com.palantir.timelock.corruption.TimeLockCorruptionNotifier;
import com.palantir.timelock.corruption.handle.LocalCorruptionHandler;
import com.palantir.timelock.history.PaxosLogHistoryProvider;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class LocalCorruptionDetector implements CorruptionDetector {
    private static final Duration TIMELOCK_CORRUPTION_ANALYSIS_INTERVAL = Duration.ofMinutes(5);
    private static final String CORRUPTION_DETECTOR_THREAD_PREFIX = "timelock-corruption-detector";

    private final ScheduledExecutorService executor = PTExecutors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory(CORRUPTION_DETECTOR_THREAD_PREFIX, true));
    private final LocalCorruptionHandler corruptionHandler;
    private final PaxosLogHistoryProvider historyProvider;
    private final LocalTimestampInvariantsVerifier timestampInvariantsVerifier;

    private volatile CorruptionStatus localCorruptionState = CorruptionStatus.HEALTHY;
    private volatile CorruptionHealthReport localCorruptionReport = CorruptionHealthReport.defaultHealthyReport();

    public static LocalCorruptionDetector create(
            PaxosLogHistoryProvider historyProvider,
            List<TimeLockCorruptionNotifier> corruptionNotifiers,
            LocalTimestampInvariantsVerifier timestampInvariants) {
        LocalCorruptionDetector localCorruptionDetector =
                new LocalCorruptionDetector(historyProvider, corruptionNotifiers, timestampInvariants);

        // TODO(mdaudali): Decide whether to re-enable once we've determined whether corruption detection will not kill
        // the node timelock is running on

        // localCorruptionDetector.scheduleWithFixedDelay();
        return localCorruptionDetector;
    }

    private LocalCorruptionDetector(
            PaxosLogHistoryProvider historyProvider,
            List<TimeLockCorruptionNotifier> corruptionNotifiers,
            LocalTimestampInvariantsVerifier timestampInvariantsVerifier) {

        this.historyProvider = historyProvider;
        this.timestampInvariantsVerifier = timestampInvariantsVerifier;
        this.corruptionHandler = new LocalCorruptionHandler(corruptionNotifiers);
    }

    @SuppressWarnings("UnusedMethod") // Remove when re-enabling local corruption
    private void scheduleWithFixedDelay() {
        executor.scheduleWithFixedDelay(
                () -> {
                    CorruptionHealthReport paxosRoundCorruptionReport = analyzeHistoryAndBuildCorruptionHealthReport();
                    CorruptionHealthReport timestampInvariantsReport =
                            timestampInvariantsVerifier.timestampInvariantsHealthReport();
                    localCorruptionReport = ImmutableCorruptionHealthReport.builder()
                            .from(paxosRoundCorruptionReport)
                            .putAllViolatingStatusesToNamespaceAndUseCase(
                                    timestampInvariantsReport.violatingStatusesToNamespaceAndUseCase())
                            .build();
                    processLocalHealthReport();
                },
                TIMELOCK_CORRUPTION_ANALYSIS_INTERVAL.getSeconds(),
                TIMELOCK_CORRUPTION_ANALYSIS_INTERVAL.getSeconds(),
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

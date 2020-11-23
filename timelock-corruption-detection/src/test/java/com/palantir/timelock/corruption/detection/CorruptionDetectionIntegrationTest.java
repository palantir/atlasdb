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

import static com.palantir.timelock.history.PaxosLogHistoryProgressTracker.MAX_ROWS_ALLOWED;

import com.google.common.collect.ImmutableSet;
import com.palantir.paxos.Client;
import com.palantir.paxos.ImmutableNamespaceAndUseCase;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.timelock.corruption.detection.TimeLockCorruptionTestSetup.StateLogComponents;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.Rule;
import org.junit.Test;

/**
 * This class performs integration tests by inducing and detecting corruption in one or more series.
 * All tests only induce and detect ACCEPTED_VALUE_GREATER_THAN_LEARNED corruption.
 */
public final class CorruptionDetectionIntegrationTest {
    @Rule
    public TimeLockCorruptionDetectionHelper helper = new TimeLockCorruptionDetectionHelper();

    @Test
    public void detectCorruptionForLogAtBatchEnd() {
        // We write logs in range [1, MAX_ROWS_ALLOWED]. The first range of sequences for corruption detection
        // = [0, MAX_ROWS_ALLOWED - 1] since this range is computed from INITIAL_PROGRESS = -1.
        helper.writeLogsOnDefaultLocalAndRemote(1, MAX_ROWS_ALLOWED);
        helper.induceGreaterAcceptedValueCorruptionOnDefaultLocalServer(MAX_ROWS_ALLOWED - 1);
        helper.assertAcceptedValueGreaterThanLearnedValue();
    }

    @Test
    public void detectCorruptionForLogInLaterBatches() {
        helper.writeLogsOnDefaultLocalAndRemote(1, MAX_ROWS_ALLOWED * 2);
        helper.induceGreaterAcceptedValueCorruptionOnDefaultLocalServer(MAX_ROWS_ALLOWED + 2);

        // No signs of corruption in the first batch
        helper.assertNoCorruptionViolations();

        // Detects signs of corruption in the second batch
        helper.assertAcceptedValueGreaterThanLearnedValue();
    }

    @Test
    public void detectCorruptionForLogAtStartOfSecondBatch() {
        // We write logs in range [1, MAX_ROWS_ALLOWED * 2]. The first range of sequences for corruption detection
        // = [0, MAX_ROWS_ALLOWED - 1] since this range is computed from INITIAL_PROGRESS = -1, which makes range of
        // the second batch = [MAX_ROWS_ALLOWED, MAX_ROWS_ALLOWED * 2 - 1].
        helper.writeLogsOnDefaultLocalAndRemote(1, MAX_ROWS_ALLOWED * 2);
        helper.induceGreaterAcceptedValueCorruptionOnDefaultLocalServer(MAX_ROWS_ALLOWED);

        // No signs of corruption in the first batch
        helper.assertNoCorruptionViolations();

        // Detects signs of corruption in the second batch
        helper.assertAcceptedValueGreaterThanLearnedValue();
    }

    @Test
    public void resetsLastVerifiedOnceGreatestKnownSeqInMemoryIsVerified() {
        helper.writeLogsOnDefaultLocalAndRemote(1, MAX_ROWS_ALLOWED / 2 + 1);

        // No signs of corruption
        helper.assertNoCorruptionViolations();

        helper.induceGreaterAcceptedValueCorruptionOnDefaultLocalServer(MAX_ROWS_ALLOWED / 2);
        // Detects signs of corruption in the now corrupt first batch of logs
        helper.assertAcceptedValueGreaterThanLearnedValue();
    }

    @Test
    public void canDetectCorruptionForLargeNumberOfLogs() {
        int greatestSeqNumber = MAX_ROWS_ALLOWED * 20;
        helper.writeLogsOnDefaultLocalAndRemote(1, greatestSeqNumber);
        helper.induceGreaterAcceptedValueCorruptionOnDefaultLocalServer(greatestSeqNumber - (MAX_ROWS_ALLOWED / 2));

        for (int i = 0; i < 19; i++) {
            helper.assertNoCorruptionViolations();
        }
        helper.assertAcceptedValueGreaterThanLearnedValue();
    }

    @Test
    public void detectCorruptionForMultipleCorruptSeries() {
        // We create 7 series and write logs to each of these in the range [1, MAX_ROWS_ALLOWED]. We then corrupt series
        // 6 & 7.
        IntStream.rangeClosed(1, 7).boxed().forEach(this::createSeriesWithPaxosLogs);
        IntStream.rangeClosed(6, 7).boxed().forEach(this::corruptSeries);

        helper.assertViolationDetectedForNamespaceAndUseCases(
                CorruptionCheckViolation.ACCEPTED_VALUE_GREATER_THAN_LEARNED,
                ImmutableSet.of(namespaceAndUseCaseForIndex(6), namespaceAndUseCaseForIndex(7)));
    }

    private void createSeriesWithPaxosLogs(int namespaceAndUseCaseIndex) {
        NamespaceAndUseCase namespaceAndUseCase = namespaceAndUseCaseForIndex(namespaceAndUseCaseIndex);
        helper.writeLogsOnLocalAndRemote(
                helper.createStatLogComponentsForNamespaceAndUseCase(namespaceAndUseCase), 1, MAX_ROWS_ALLOWED);
    }

    private void corruptSeries(int namespaceAndUseCaseIndex) {
        NamespaceAndUseCase namespaceAndUseCase = namespaceAndUseCaseForIndex(namespaceAndUseCaseIndex);
        List<StateLogComponents> components = helper.createStatLogComponentsForNamespaceAndUseCase(namespaceAndUseCase);
        helper.induceGreaterAcceptedValueCorruption(components.get(0), MAX_ROWS_ALLOWED - 1);
    }

    private static NamespaceAndUseCase namespaceAndUseCaseForIndex(int index) {
        return ImmutableNamespaceAndUseCase.of(Client.of("client_" + index), "client");
    }
}

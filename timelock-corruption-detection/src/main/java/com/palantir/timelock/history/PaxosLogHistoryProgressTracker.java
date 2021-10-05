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

package com.palantir.timelock.history;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.timelock.history.models.LearnerUseCase;
import com.palantir.timelock.history.models.ProgressState;
import com.palantir.timelock.history.sqlite.LogVerificationProgressState;
import com.palantir.timelock.history.sqlite.SqlitePaxosStateLogHistory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;

public class PaxosLogHistoryProgressTracker {
    @SuppressWarnings("VisibleForTestingPackagePrivate") // used in corruption-detection tests
    @VisibleForTesting
    public static final int MAX_ROWS_ALLOWED = 250;

    private final LogVerificationProgressState logVerificationProgressState;
    private final SqlitePaxosStateLogHistory sqlitePaxosStateLogHistory;

    private Map<NamespaceAndUseCase, ProgressState> verificationProgressStateCache = new ConcurrentHashMap<>();

    public PaxosLogHistoryProgressTracker(
            DataSource dataSource, SqlitePaxosStateLogHistory sqlitePaxosStateLogHistory) {
        this.logVerificationProgressState = LogVerificationProgressState.create(dataSource);
        this.sqlitePaxosStateLogHistory = sqlitePaxosStateLogHistory;
    }

    public HistoryQuerySequenceBounds getNextPaxosLogSequenceRangeToBeVerified(
            NamespaceAndUseCase namespaceAndUseCase) {
        ProgressState progressState = getOrPopulateProgressState(namespaceAndUseCase);

        if (progressState.shouldResetProgressState()) {
            progressState = resetProgressState(namespaceAndUseCase);
        }

        return sequenceBoundsForNextHistoryQuery(progressState.lastVerifiedSeq());
    }

    public void updateProgressState(
            Map<NamespaceAndUseCase, HistoryQuerySequenceBounds> namespaceAndUseCaseWiseLoadedSequenceRange) {
        namespaceAndUseCaseWiseLoadedSequenceRange.forEach(this::updateProgressStateForNamespaceAndUseCase);
    }

    private ProgressState getOrPopulateProgressState(NamespaceAndUseCase namespaceAndUseCase) {
        return verificationProgressStateCache.computeIfAbsent(
                namespaceAndUseCase, this::progressStatusForNamespaceAndUseCase);
    }

    private ProgressState progressStatusForNamespaceAndUseCase(NamespaceAndUseCase namespaceAndUseCase) {
        long lastVerifiedSeq = logVerificationProgressState.getLastVerifiedSeq(
                namespaceAndUseCase.namespace(), namespaceAndUseCase.useCase());
        return buildProgressStatusWithLastVerifiedSeqForNamespaceAndUseCase(namespaceAndUseCase, lastVerifiedSeq);
    }

    @VisibleForTesting
    void updateProgressStateForNamespaceAndUseCase(
            NamespaceAndUseCase namespaceAndUseCase, HistoryQuerySequenceBounds bounds) {
        updateProgressInDbThroughCache(
                namespaceAndUseCase, bounds.getUpperBoundInclusive(), getOrPopulateProgressState(namespaceAndUseCase));
    }

    private ProgressState updateProgressInDbThroughCache(
            NamespaceAndUseCase namespaceAndUseCase, long lastVerifiedSequence, ProgressState currentState) {
        ProgressState newProgressState = ProgressState.builder()
                .greatestSeqNumberToBeVerified(currentState.greatestSeqNumberToBeVerified())
                .lastVerifiedSeq(lastVerifiedSequence)
                .build();
        verificationProgressStateCache.put(namespaceAndUseCase, newProgressState);
        logVerificationProgressState.updateProgress(
                namespaceAndUseCase.namespace(), namespaceAndUseCase.useCase(), lastVerifiedSequence);
        return newProgressState;
    }

    private ProgressState resetProgressState(NamespaceAndUseCase namespaceAndUseCase) {
        logVerificationProgressState.setInitialProgress(namespaceAndUseCase.namespace(), namespaceAndUseCase.useCase());
        ProgressState resetProgressState = buildProgressStatusWithLastVerifiedSeqForNamespaceAndUseCase(
                namespaceAndUseCase, LogVerificationProgressState.INITIAL_PROGRESS);
        verificationProgressStateCache.put(namespaceAndUseCase, resetProgressState);
        return resetProgressState;
    }

    private long getLatestLearnedSequenceForNamespaceAndUseCase(NamespaceAndUseCase namespaceAndUseCase) {
        return sqlitePaxosStateLogHistory.getGreatestLogEntry(
                namespaceAndUseCase.namespace(), LearnerUseCase.createLearnerUseCase(namespaceAndUseCase.useCase()));
    }

    private ProgressState buildProgressStatusWithLastVerifiedSeqForNamespaceAndUseCase(
            NamespaceAndUseCase namespaceAndUseCase, long lastVerifiedSeq) {
        return ProgressState.builder()
                .lastVerifiedSeq(lastVerifiedSeq)
                .greatestSeqNumberToBeVerified(getLatestLearnedSequenceForNamespaceAndUseCase(namespaceAndUseCase))
                .build();
    }

    private HistoryQuerySequenceBounds sequenceBoundsForNextHistoryQuery(long lastVerified) {
        return HistoryQuerySequenceBounds.of(lastVerified + 1, lastVerified + MAX_ROWS_ALLOWED);
    }
}

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
import com.palantir.paxos.Client;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.timelock.history.models.LearnerUseCase;
import com.palantir.timelock.history.models.ProgressState;
import com.palantir.timelock.history.models.SequenceBounds;
import com.palantir.timelock.history.sqlite.LogVerificationProgressState;
import com.palantir.timelock.history.sqlite.SqlitePaxosStateLogHistory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;

public class PaxosLogHistoryProgressTracker {
    private final LogVerificationProgressState logVerificationProgressState;
    private final SqlitePaxosStateLogHistory sqlitePaxosStateLogHistory;

    private Map<NamespaceAndUseCase, ProgressState> verificationProgressStateCache = new ConcurrentHashMap<>();

    public PaxosLogHistoryProgressTracker(
            DataSource dataSource, SqlitePaxosStateLogHistory sqlitePaxosStateLogHistory) {
        this.logVerificationProgressState = LogVerificationProgressState.create(dataSource);
        this.sqlitePaxosStateLogHistory = sqlitePaxosStateLogHistory;
    }

    public SequenceBounds getPaxosLogSequenceBounds(NamespaceAndUseCase namespaceAndUseCase) {
        ProgressState progressState = getOrPopulateProgressState(namespaceAndUseCase);

        if (progressState.shouldResetProgressState()) {
            progressState = resetProgressState(namespaceAndUseCase);
        }

        return SequenceBounds.getBoundsSinceLastVerified(progressState.lastVerifiedSeq());
    }

    public void updateProgressState(Map<NamespaceAndUseCase, SequenceBounds> namespaceAndUseCaseSequenceBoundsMap) {
        namespaceAndUseCaseSequenceBoundsMap.forEach(this::updateProgressStateForNamespaceAndUseCase);
    }

    private ProgressState getOrPopulateProgressState(NamespaceAndUseCase namespaceAndUseCase) {
        return verificationProgressStateCache.computeIfAbsent(namespaceAndUseCase, this::getLastVerifiedSeqFromLogs);
    }

    private ProgressState getLastVerifiedSeqFromLogs(NamespaceAndUseCase namespaceAndUseCase) {
        Client client = namespaceAndUseCase.namespace();
        String useCase = namespaceAndUseCase.useCase();

        return logVerificationProgressState
                .getProgressState(client, useCase)
                .orElseGet(() -> logVerificationProgressState.resetProgressState(
                        client, useCase, getLatestLearnedSequenceForNamespaceAndUseCase(namespaceAndUseCase)));
    }

    @VisibleForTesting
    void updateProgressStateForNamespaceAndUseCase(NamespaceAndUseCase namespaceAndUseCase, SequenceBounds bounds) {
        updateProgressInDbThroughCache(
                namespaceAndUseCase, bounds.upperInclusive(), getOrPopulateProgressState(namespaceAndUseCase));
    }

    private ProgressState updateProgressInDbThroughCache(
            NamespaceAndUseCase namespaceAndUseCase, long lastVerifiedSequence, ProgressState state) {
        ProgressState progressState = ProgressState.builder()
                .greatestSeqNumberToBeVerified(state.greatestSeqNumberToBeVerified())
                .lastVerifiedSeq(lastVerifiedSequence)
                .build();
        verificationProgressStateCache.put(namespaceAndUseCase, progressState);
        logVerificationProgressState.updateProgress(
                namespaceAndUseCase.namespace(), namespaceAndUseCase.useCase(), lastVerifiedSequence);
        return progressState;
    }

    private ProgressState resetProgressState(NamespaceAndUseCase namespaceAndUseCase) {
        ProgressState resetProgressState = logVerificationProgressState.resetProgressState(
                namespaceAndUseCase.namespace(),
                namespaceAndUseCase.useCase(),
                getLatestLearnedSequenceForNamespaceAndUseCase(namespaceAndUseCase));
        verificationProgressStateCache.put(namespaceAndUseCase, resetProgressState);
        return resetProgressState;
    }

    private long getLatestLearnedSequenceForNamespaceAndUseCase(NamespaceAndUseCase namespaceAndUseCase) {
        return sqlitePaxosStateLogHistory.getGreatestLogEntry(
                namespaceAndUseCase.namespace(), LearnerUseCase.createLearnerUseCase(namespaceAndUseCase.useCase()));
    }
}

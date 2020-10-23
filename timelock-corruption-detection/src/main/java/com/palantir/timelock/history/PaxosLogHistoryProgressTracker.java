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
import com.palantir.timelock.history.models.ProgressComponents;
import com.palantir.timelock.history.models.SequenceBounds;
import com.palantir.timelock.history.sqlite.LogVerificationProgressState;
import com.palantir.timelock.history.sqlite.SqlitePaxosStateLogHistory;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;

public class PaxosLogHistoryProgressTracker {
    public static final int MAX_ROWS_ALLOWED = 500;

    private final LogVerificationProgressState logVerificationProgressState;
    private final SqlitePaxosStateLogHistory sqlitePaxosStateLogHistory;

    private Map<NamespaceAndUseCase, ProgressComponents> verificationProgressStateCache = new ConcurrentHashMap<>();

    public PaxosLogHistoryProgressTracker(
            DataSource dataSource, SqlitePaxosStateLogHistory sqlitePaxosStateLogHistory) {
        this.logVerificationProgressState = LogVerificationProgressState.create(dataSource);
        this.sqlitePaxosStateLogHistory = sqlitePaxosStateLogHistory;
    }

    public SequenceBounds getPaxosLogSequenceBounds(NamespaceAndUseCase namespaceAndUseCase) {
        ProgressComponents progress = getOrPopulateProgressComponents(namespaceAndUseCase);
        return SequenceBounds.builder()
                .lower(progress.progressState())
                .upper(progress.progressState() + MAX_ROWS_ALLOWED)
                .build();
    }

    public void updateProgressState(Map<NamespaceAndUseCase, SequenceBounds> namespaceAndUseCaseSequenceBoundsMap) {
        namespaceAndUseCaseSequenceBoundsMap.forEach((namespaceAndUseCase, bounds) ->
                updateProgressStateForNamespaceAndUseCase(namespaceAndUseCase, bounds));
    }

    private ProgressComponents getOrPopulateProgressComponents(NamespaceAndUseCase namespaceAndUseCase) {
        return verificationProgressStateCache.computeIfAbsent(namespaceAndUseCase, this::getLastVerifiedSeqFromLogs);
    }

    private ProgressComponents getLastVerifiedSeqFromLogs(NamespaceAndUseCase namespaceAndUseCase) {
        Client client = namespaceAndUseCase.namespace();
        String useCase = namespaceAndUseCase.useCase();

        return logVerificationProgressState
                .getProgressComponents(client, useCase)
                .orElseGet(() -> logVerificationProgressState.resetProgressState(
                        client, useCase, getLatestSequenceForNamespaceAndUseCase(namespaceAndUseCase)));
    }

    @VisibleForTesting
    void updateProgressStateForNamespaceAndUseCase(NamespaceAndUseCase key, SequenceBounds value) {
        long lastVerifiedSequence = value.upper();

        ProgressComponents currentProgressState = getOrPopulateProgressComponents(key);
        resetIfRequired(key, value, currentProgressState)
                .orElseGet(
                        () -> updateProgressInDbThroughCache(key, value, lastVerifiedSequence, currentProgressState));
    }

    private ProgressComponents updateProgressInDbThroughCache(
            NamespaceAndUseCase key,
            SequenceBounds value,
            long lastVerifiedSequence,
            ProgressComponents progressComponents) {
        ProgressComponents progressState = ProgressComponents.builder()
                .progressLimit(progressComponents.progressLimit())
                .progressState(value.upper())
                .build();
        verificationProgressStateCache.put(key, progressState);
        logVerificationProgressState.updateProgress(key.namespace(), key.useCase(), lastVerifiedSequence);
        return progressState;
    }

    private Optional<ProgressComponents> resetIfRequired(
            NamespaceAndUseCase key, SequenceBounds value, ProgressComponents progressComponents) {
        if (value.upper() <= progressComponents.progressLimit()) {
            return Optional.empty();
        }
        return Optional.of(verificationProgressStateCache.put(
                key,
                logVerificationProgressState.resetProgressState(
                        key.namespace(), key.useCase(), getLatestSequenceForNamespaceAndUseCase(key))));
    }

    private long getLatestSequenceForNamespaceAndUseCase(NamespaceAndUseCase namespaceAndUseCase) {
        return sqlitePaxosStateLogHistory.getGreatestLogEntry(
                namespaceAndUseCase.namespace(), namespaceAndUseCase.useCase());
    }
}

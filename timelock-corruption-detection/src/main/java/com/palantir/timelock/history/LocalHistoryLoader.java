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
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.timelock.history.models.AcceptorUseCase;
import com.palantir.timelock.history.models.ImmutablePaxosHistoryOnSingleNode;
import com.palantir.timelock.history.models.LearnerAndAcceptorRecords;
import com.palantir.timelock.history.models.LearnerUseCase;
import com.palantir.timelock.history.models.PaxosHistoryOnSingleNode;
import com.palantir.timelock.history.sqlite.SqlitePaxosStateLogHistory;
import java.util.Map;

// TBD cache implementation
public final class LocalHistoryLoader {
    private final SqlitePaxosStateLogHistory sqlitePaxosStateLogHistory;

    private LocalHistoryLoader(SqlitePaxosStateLogHistory sqlitePaxosStateLogHistory) {
        this.sqlitePaxosStateLogHistory = sqlitePaxosStateLogHistory;
    }

    public static LocalHistoryLoader create(SqlitePaxosStateLogHistory sqlitePaxosStateLogHistory) {
        return new LocalHistoryLoader(sqlitePaxosStateLogHistory);
    }

    public PaxosHistoryOnSingleNode getLocalPaxosHistory(Map<NamespaceAndUseCase, Long> lastVerifiedSequences) {
        return ImmutablePaxosHistoryOnSingleNode.of(KeyedStream.stream(lastVerifiedSequences)
                .map(this::loadLocalHistory)
                .collectToMap());
    }

    @VisibleForTesting
    LearnerAndAcceptorRecords loadLocalHistory(NamespaceAndUseCase namespaceAndUseCase, Long seq) {
        String paxosUseCasePrefix = namespaceAndUseCase.useCase();
        return sqlitePaxosStateLogHistory.getLearnerAndAcceptorLogsSince(
                namespaceAndUseCase.namespace(),
                LearnerUseCase.createLearnerUseCase(paxosUseCasePrefix),
                AcceptorUseCase.createAcceptorUseCase(paxosUseCasePrefix),
                seq);
    }
}

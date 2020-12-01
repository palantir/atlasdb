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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.ImmutableNamespaceAndUseCase;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.paxos.PaxosValue;
import com.palantir.timelock.history.models.LearnerUseCase;
import com.palantir.timelock.history.sqlite.SqlitePaxosStateLogHistory;
import com.palantir.timelock.history.util.UseCaseUtils;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sql.DataSource;
import one.util.streamex.StreamEx;

public class LocalTimestampInvariantsVerifier {
    @VisibleForTesting
    public static final int LEARNER_LOG_BATCH_SIZE_LIMIT = 250;

    private final SqlitePaxosStateLogHistory sqlitePaxosStateLogHistory;
    private Map<NamespaceAndUseCase, Long> minSeqBoundToBeVerified = new ConcurrentHashMap<>();

    public LocalTimestampInvariantsVerifier(DataSource dataSource) {
        this.sqlitePaxosStateLogHistory = SqlitePaxosStateLogHistory.create(dataSource);
    }

    public CorruptionHealthReport timestampInvariantsHealthReport() {
        SetMultimap<CorruptionCheckViolation, NamespaceAndUseCase> namespacesExhibitingViolations = KeyedStream.of(
                        getNamespaceAndUseCaseTuples())
                .map(this::timestampInvariantsViolationLevel)
                .filter(CorruptionCheckViolation::raiseErrorAlert)
                .mapEntries((k, v) -> Maps.immutableEntry(v, k))
                .collectToSetMultimap();
        return ImmutableCorruptionHealthReport.builder()
                .violatingStatusesToNamespaceAndUseCase(namespacesExhibitingViolations)
                .build();
    }

    private CorruptionCheckViolation timestampInvariantsViolationLevel(NamespaceAndUseCase namespaceAndUseCase) {
        Stream<Long> expectedSortedTimestamps = KeyedStream.stream(getLearnerLogs(namespaceAndUseCase))
                .map(PaxosValue::getData)
                .filter(Predicates.notNull())
                .mapEntries((sequence, timestamp) -> Maps.immutableEntry(sequence, PtBytes.toLong(timestamp)))
                .entries()
                .sorted(Comparator.comparingLong(Map.Entry::getKey))
                .map(Map.Entry::getValue);
        return StreamEx.of(expectedSortedTimestamps)
                        .pairMap((first, second) -> first >= second)
                        .anyMatch(x -> x)
                ? CorruptionCheckViolation.CLOCK_WENT_BACKWARDS
                : CorruptionCheckViolation.NONE;
    }

    private Map<Long, PaxosValue> getLearnerLogs(NamespaceAndUseCase namespaceAndUseCase) {
        long minSeqToBeVerified = getMinSeqToBeVerified(namespaceAndUseCase);
        LearnerUseCase useCase = LearnerUseCase.createLearnerUseCase(namespaceAndUseCase.useCase());

        Map<Long, PaxosValue> learnerLogsSince = sqlitePaxosStateLogHistory.getLearnerLogsSince(
                namespaceAndUseCase.namespace(), useCase, minSeqToBeVerified, LEARNER_LOG_BATCH_SIZE_LIMIT);

        if (notEnoughLogsForVerification(learnerLogsSince)) {
            resetMinSequenceToBeVerified(namespaceAndUseCase);
        } else {
            updateMinSeqToBeVerified(namespaceAndUseCase, learnerLogsSince);
        }
        return learnerLogsSince;
    }

    private boolean notEnoughLogsForVerification(Map<Long, PaxosValue> learnerLogsSince) {
        return learnerLogsSince.size() <= 1;
    }

    private void updateMinSeqToBeVerified(NamespaceAndUseCase namespaceAndUseCase, Map<Long, PaxosValue> minSeq) {
        minSeqBoundToBeVerified.put(namespaceAndUseCase, Collections.max(minSeq.keySet()));
    }

    private long getMinSeqToBeVerified(NamespaceAndUseCase namespaceAndUseCase) {
        return minSeqBoundToBeVerified.computeIfAbsent(namespaceAndUseCase, _u -> -1L);
    }

    private void resetMinSequenceToBeVerified(NamespaceAndUseCase namespaceAndUseCase) {
        minSeqBoundToBeVerified.put(namespaceAndUseCase, -1L);
    }

    private ImmutableNamespaceAndUseCase getNamespaceAndUseCasePrefix(NamespaceAndUseCase namespaceAndUseCase) {
        return ImmutableNamespaceAndUseCase.of(
                namespaceAndUseCase.namespace(), UseCaseUtils.getPaxosUseCasePrefix(namespaceAndUseCase.useCase()));
    }

    private Set<NamespaceAndUseCase> getNamespaceAndUseCaseTuples() {
        return sqlitePaxosStateLogHistory.getAllNamespaceAndUseCaseTuples().stream()
                .map(this::getNamespaceAndUseCasePrefix)
                .collect(Collectors.toSet());
    }
}

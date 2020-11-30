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
import com.palantir.timelock.history.HistoryQuerySequenceBounds;
import com.palantir.timelock.history.PaxosLogHistoryProgressTracker;
import com.palantir.timelock.history.models.LearnerUseCase;
import com.palantir.timelock.history.sqlite.SqlitePaxosStateLogHistory;
import com.palantir.timelock.history.util.UseCaseUtils;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sql.DataSource;
import one.util.streamex.StreamEx;

public class LocalTimestampInvariantsVerifier {
    @VisibleForTesting
    static final int DELTA = 5;

    private final SqlitePaxosStateLogHistory sqlitePaxosStateLogHistory;
    private final PaxosLogHistoryProgressTracker progressTracker;

    public LocalTimestampInvariantsVerifier(DataSource dataSource) {
        this.sqlitePaxosStateLogHistory = SqlitePaxosStateLogHistory.create(dataSource);
        this.progressTracker = new PaxosLogHistoryProgressTracker(dataSource, sqlitePaxosStateLogHistory);
    }

    public CorruptionHealthReport timestampInvariantsHealthReport() {
        Set<NamespaceAndUseCase> corruptNamespaces = KeyedStream.stream(
                        getNamespaceAndUseCaseToHistoryQuerySeqBoundsMap())
                .filterEntries(this::timestampWentBackwardsForNamespace)
                .keys()
                .collect(Collectors.toSet());
        SetMultimap<CorruptionCheckViolation, NamespaceAndUseCase> namespacesExhibitingViolations = KeyedStream.of(
                        corruptNamespaces)
                .mapKeys(_u -> CorruptionCheckViolation.CLOCK_WENT_BACKWARDS)
                .collectToSetMultimap();
        return ImmutableCorruptionHealthReport.builder()
                .violatingStatusesToNamespaceAndUseCase(namespacesExhibitingViolations)
                .build();
    }

    private boolean timestampWentBackwardsForNamespace(
            NamespaceAndUseCase namespaceAndUseCase, HistoryQuerySequenceBounds historyQuerySequenceBounds) {
        LearnerUseCase useCase = LearnerUseCase.createLearnerUseCase(namespaceAndUseCase.useCase());
        Map<Long, PaxosValue> learnerLogsInRange = sqlitePaxosStateLogHistory.getLearnerLogsInRange(
                namespaceAndUseCase.namespace(), useCase, getQueryBoundsWithDelta(historyQuerySequenceBounds));
        Stream<Long> expectedSortedTimestamps = KeyedStream.stream(learnerLogsInRange)
                .map(PaxosValue::getData)
                .filter(Predicates.notNull())
                .mapEntries((sequence, timestamp) -> Maps.immutableEntry(sequence, PtBytes.toLong(timestamp)))
                .entries()
                .sorted(Comparator.comparingLong(Map.Entry::getKey))
                .map(Map.Entry::getValue);
        return StreamEx.of(expectedSortedTimestamps)
                .pairMap((first, second) -> first >= second)
                .anyMatch(x -> x);
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

    private Map<NamespaceAndUseCase, HistoryQuerySequenceBounds> getNamespaceAndUseCaseToHistoryQuerySeqBoundsMap() {
        return KeyedStream.of(getNamespaceAndUseCaseTuples().stream())
                .map(progressTracker::getNextPaxosLogSequenceRangeToBeVerified)
                .collectToMap();
    }

    private HistoryQuerySequenceBounds getQueryBoundsWithDelta(HistoryQuerySequenceBounds sequenceBounds) {
        return HistoryQuerySequenceBounds.of(
                sequenceBounds.getLowerBoundInclusive() - DELTA, sequenceBounds.getUpperBoundInclusive() + DELTA);
    }
}

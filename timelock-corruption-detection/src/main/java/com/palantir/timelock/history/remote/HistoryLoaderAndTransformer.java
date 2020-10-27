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

package com.palantir.timelock.history.remote;

import com.google.common.collect.Maps;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.timelock.history.HistoryQuery;
import com.palantir.timelock.history.HistoryQuerySequenceBounds;
import com.palantir.timelock.history.LocalHistoryLoader;
import com.palantir.timelock.history.LogsForNamespaceAndUseCase;
import com.palantir.timelock.history.PaxosLogWithAcceptedAndLearnedValues;
import com.palantir.timelock.history.models.LearnerAndAcceptorRecords;
import com.palantir.timelock.history.models.PaxosHistoryOnSingleNode;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class HistoryLoaderAndTransformer {
    private HistoryLoaderAndTransformer() {
        // no op
    }

    public static List<LogsForNamespaceAndUseCase> getLogsForHistoryQueries(
            LocalHistoryLoader localHistoryLoader, List<HistoryQuery> historyQueries) {
        Map<NamespaceAndUseCase, HistoryQuerySequenceBounds> namespaceAndUseCaseWiseSequenceRangeToBeVerified =
                historyQueries.stream()
                        .collect(Collectors.toMap(
                                HistoryQuery::getNamespaceAndUseCase,
                                HistoryQuery::getSequenceBounds,
                                HistoryLoaderAndTransformer::minimalLowerBoundResolver));

        PaxosHistoryOnSingleNode localPaxosHistory =
                localHistoryLoader.getLocalPaxosHistory(namespaceAndUseCaseWiseSequenceRangeToBeVerified);

        return KeyedStream.stream(localPaxosHistory.history())
                .mapEntries(HistoryLoaderAndTransformer::processHistory)
                .values()
                .collect(Collectors.toList());
    }

    private static HistoryQuerySequenceBounds minimalLowerBoundResolver(
            HistoryQuerySequenceBounds bound1, HistoryQuerySequenceBounds bound2) {
        return bound1.getLowerBoundInclusive() < bound2.getLowerBoundInclusive() ? bound1 : bound2;
    }

    private static Map.Entry<NamespaceAndUseCase, LogsForNamespaceAndUseCase> processHistory(
            NamespaceAndUseCase namespaceAndUseCase, LearnerAndAcceptorRecords records) {

        List<PaxosLogWithAcceptedAndLearnedValues> logs = records.getAllSequenceNumbers().stream()
                .map(sequence -> PaxosLogWithAcceptedAndLearnedValues.builder()
                        .paxosValue(records.getLearnedValueAtSeqIfExists(sequence))
                        .acceptedState(records.getAcceptedValueAtSeqIfExists(sequence))
                        .seq(sequence)
                        .build())
                .collect(Collectors.toList());

        return Maps.immutableEntry(namespaceAndUseCase, LogsForNamespaceAndUseCase.of(namespaceAndUseCase, logs));
    }
}

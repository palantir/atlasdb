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

package com.palantir.history;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.history.models.LearnerAndAcceptorRecords;
import com.palantir.history.models.PaxosHistoryOnSingleNode;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.timelock.history.HistoryQuery;
import com.palantir.timelock.history.LogsForNamespaceAndUseCase;
import com.palantir.timelock.history.PaxosLogWithAcceptedAndLearnedValues;
import com.palantir.timelock.history.TimeLockPaxosHistoryProvider;
import com.palantir.timelock.history.TimeLockPaxosHistoryProviderEndpoints;
import com.palantir.timelock.history.UndertowTimeLockPaxosHistoryProvider;
import com.palantir.tokens.auth.AuthHeader;

public class TimeLockPaxosHistoryProviderResource implements UndertowTimeLockPaxosHistoryProvider {
    private LocalHistoryLoader localHistoryLoader;

    @VisibleForTesting
    TimeLockPaxosHistoryProviderResource(LocalHistoryLoader localHistoryLoader) {
        this.localHistoryLoader = localHistoryLoader;
    }

    @Override
    public ListenableFuture<List<LogsForNamespaceAndUseCase>> getPaxosHistory(AuthHeader authHeader,
            List<HistoryQuery> historyQueries) {
        Map<NamespaceAndUseCase, Long> lastVerifiedSequences = historyQueries.stream().collect(
                Collectors.toMap(HistoryQuery::getNamespaceAndUseCase, HistoryQuery::getSeq, Math::min));

        PaxosHistoryOnSingleNode localPaxosHistory = localHistoryLoader.getLocalPaxosHistory(lastVerifiedSequences);

        List<LogsForNamespaceAndUseCase> logsForNamespaceAndUseCases = KeyedStream.stream(localPaxosHistory.history())
                .mapEntries(this::processHistory)
                .values()
                .collect(Collectors.toList());
        return Futures.immediateFuture(logsForNamespaceAndUseCases);
    }

    public Map.Entry<NamespaceAndUseCase, LogsForNamespaceAndUseCase> processHistory(
            NamespaceAndUseCase namespaceAndUseCase, LearnerAndAcceptorRecords records) {

        long minSeq = records.getMinSequence();
        long maxSeq = records.getMaxSequence();

        List<PaxosLogWithAcceptedAndLearnedValues> logs = LongStream.rangeClosed(minSeq, maxSeq).boxed().map(
                sequence -> PaxosLogWithAcceptedAndLearnedValues.builder()
                        .paxosValue(records.getLearnedValueAtSeqIfExists(sequence))
                        .acceptedState(records.getAcceptedValueAtSeqIfExists(sequence))
                        .seq(sequence)
                        .build()).collect(Collectors.toList());

        return Maps.immutableEntry(namespaceAndUseCase, LogsForNamespaceAndUseCase.of(namespaceAndUseCase, logs));
    }

    public static UndertowService undertow(LocalHistoryLoader localHistoryLoader) {
        return TimeLockPaxosHistoryProviderEndpoints.of(new TimeLockPaxosHistoryProviderResource(localHistoryLoader));
    }

    public static TimeLockPaxosHistoryProvider jersey(LocalHistoryLoader localHistoryLoader) {
        return new JerseyAdapter(new TimeLockPaxosHistoryProviderResource(localHistoryLoader));
    }

    public static class JerseyAdapter implements TimeLockPaxosHistoryProvider {
        private final TimeLockPaxosHistoryProviderResource delegate;

        private JerseyAdapter(TimeLockPaxosHistoryProviderResource delegate) {
            this.delegate = delegate;
        }

        @Override
        public List<LogsForNamespaceAndUseCase> getPaxosHistory(AuthHeader authHeader,
                List<HistoryQuery> historyQueries) {
            return AtlasFutures.getUnchecked(delegate.getPaxosHistory(authHeader, historyQueries));
        }
    }
}
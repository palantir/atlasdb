/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.paxos;

import com.google.common.collect.ImmutableList;
import com.palantir.leader.PaxosKnowledgeEventRecorder;
import com.palantir.logsafe.SafeArg;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PaxosLearnerImpl implements PaxosLearner {
    private static final Logger log = LoggerFactory.getLogger(PaxosLearnerImpl.class);

    public static PaxosLearner newLearner(String logDir, PaxosKnowledgeEventRecorder eventRecorder) {
        return newLearner(new PaxosStateLogImpl<>(logDir), eventRecorder);
    }

    private static PaxosLearner newLearner(
            PaxosStateLog<PaxosValue> stateLog, PaxosKnowledgeEventRecorder eventRecorder) {
        ConcurrentSkipListMap<Long, PaxosValue> state = new ConcurrentSkipListMap<>();

        byte[] greatestValidValue = PaxosStateLogs.getGreatestValidLogEntry(stateLog);
        if (greatestValidValue != null) {
            PaxosValue value = PaxosValue.BYTES_HYDRATOR.hydrateFromBytes(greatestValidValue);
            state.put(value.getRound(), value);
        }

        return new PaxosLearnerImpl(state, stateLog, eventRecorder);
    }

    public static PaxosLearner newSqliteLearner(
            PaxosStorageParameters params,
            SplittingPaxosStateLog.LegacyOperationMarkers _legacyOperationMarkers,
            PaxosKnowledgeEventRecorder event) {
        PaxosStateLog<PaxosValue> stateLog =
                SqlitePaxosStateLog.create(params.namespaceAndUseCase(), params.sqliteDataSource());
        return newLearner(stateLog, event);
    }

    final SortedMap<Long, PaxosValue> state;
    final PaxosStateLog<PaxosValue> learnerStateLog;
    final PaxosKnowledgeEventRecorder eventRecorder;

    private PaxosLearnerImpl(
            SortedMap<Long, PaxosValue> stateWithGreatestValueFromLog,
            PaxosStateLog<PaxosValue> learnerStateLog,
            PaxosKnowledgeEventRecorder eventRecorder) {
        this.state = stateWithGreatestValueFromLog;
        this.learnerStateLog = learnerStateLog;
        this.eventRecorder = eventRecorder;
    }

    @Override
    public void learn(long seq, PaxosValue val) {
        state.put(seq, val);
        learnerStateLog.writeRound(seq, val);
        eventRecorder.recordRound(val);
    }

    @Override
    public Optional<PaxosValue> getLearnedValue(long seq) {
        try {
            if (!state.containsKey(seq)) {
                byte[] bytes = learnerStateLog.readRound(seq);
                if (bytes != null) {
                    PaxosValue value = PaxosValue.BYTES_HYDRATOR.hydrateFromBytes(learnerStateLog.readRound(seq));
                    state.put(seq, value);
                }
            }
            return Optional.ofNullable(state.get(seq));
        } catch (IOException e) {
            log.error("Unable to get corrupt learned value for sequence {}", SafeArg.of("sequence", seq), e);
            return Optional.empty();
        }
    }

    @Override
    public Collection<PaxosValue> getLearnedValuesSince(long seq) {
        Optional<Long> greatestSeq = getGreatestLearnedValue().map(PaxosValue::getRound);
        if (!greatestSeq.isPresent()) {
            return ImmutableList.of();
        }

        return LongStream.rangeClosed(seq, greatestSeq.get())
                .boxed()
                .map(this::getLearnedValue)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    @Override
    public Optional<PaxosValue> getGreatestLearnedValue() {
        if (state.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(state.get(state.lastKey()));
    }
}

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.leader.PaxosKnowledgeEventRecorder;
import com.palantir.logsafe.SafeArg;

public final class PaxosLearnerImpl implements PaxosLearner {

    private static final Logger logger = LoggerFactory.getLogger(PaxosLearnerImpl.class);


    public static PaxosLearner newLearner(String logDir) {
        return newLearner(logDir, PaxosKnowledgeEventRecorder.NO_OP);
    }

    public static PaxosLearner newLearner(String logDir, PaxosKnowledgeEventRecorder eventRecorder) {
        PaxosStateLogImpl<PaxosValue> log = new PaxosStateLogImpl<PaxosValue>(logDir);
        ConcurrentSkipListMap<Long, PaxosValue> state = new ConcurrentSkipListMap<Long, PaxosValue>();

        byte[] greatestValidValue = PaxosStateLogs.getGreatestValidLogEntry(log);
        if (greatestValidValue != null) {
            PaxosValue value = PaxosValue.BYTES_HYDRATOR.hydrateFromBytes(greatestValidValue);
            state.put(value.getRound(), value);
        }

        return new PaxosLearnerImpl(state, log, eventRecorder);
    }

    final SortedMap<Long, PaxosValue> state;
    final PaxosStateLog<PaxosValue> log;
    final PaxosKnowledgeEventRecorder eventRecorder;

    private PaxosLearnerImpl(SortedMap<Long, PaxosValue> stateWithGreatestValueFromLog,
                             PaxosStateLog<PaxosValue> log,
                            PaxosKnowledgeEventRecorder eventRecorder) {
        this.state = stateWithGreatestValueFromLog;
        this.log = log;
        this.eventRecorder = eventRecorder;
    }

    @Override
    public void learn(long seq, PaxosValue val) {
        state.put(seq, val);
        log.writeRound(seq, val);
        eventRecorder.recordRound(val);
    }

    @Override
    public PaxosValue getLearnedValue(long seq) {
        try {
            if (!state.containsKey(seq)) {
                byte[] bytes = log.readRound(seq);
                if (bytes != null) {
                    PaxosValue value = PaxosValue.BYTES_HYDRATOR.hydrateFromBytes(log.readRound(seq));
                    state.put(seq, value);
                }
            }
            return state.get(seq);
        } catch (IOException e) {
            logger.error("Unable to get corrupt learned value for sequence {}",
                    SafeArg.of("sequence", seq), e);
            return null;
        }
    }

    @Override
    public Collection<PaxosValue> getLearnedValuesSince(long seq) {
        PaxosValue greatestLearnedValue = getGreatestLearnedValue();
        long greatestSeq = -1L;
        if (greatestLearnedValue != null) {
            greatestSeq = greatestLearnedValue.seq;
        }

        Collection<PaxosValue> values = new ArrayList<PaxosValue>();
        for (long i = seq; i <= greatestSeq; i++) {
            PaxosValue value;
            value = getLearnedValue(i);
            if (value != null) {
                values.add(value);
            }
        }
        return values;
    }

    @Override
    public PaxosValue getGreatestLearnedValue() {
        if (!state.isEmpty()) {
            return state.get(state.lastKey());
        }
        return null;
    }
}

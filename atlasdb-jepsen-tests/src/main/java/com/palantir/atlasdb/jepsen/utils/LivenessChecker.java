/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.jepsen.utils;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.jepsen.CheckerResult;
import com.palantir.atlasdb.jepsen.ImmutableCheckerResult;
import com.palantir.atlasdb.jepsen.events.Checker;
import com.palantir.atlasdb.jepsen.events.Event;
import com.palantir.atlasdb.jepsen.events.EventVisitor;
import com.palantir.atlasdb.jepsen.events.FailEvent;
import com.palantir.atlasdb.jepsen.events.ImmutableInfoEvent;
import com.palantir.atlasdb.jepsen.events.InfoEvent;
import com.palantir.atlasdb.jepsen.events.InvokeEvent;
import com.palantir.atlasdb.jepsen.events.OkEvent;
import java.util.List;
import java.util.function.Predicate;

public class LivenessChecker implements Checker {
    private final Predicate<OkEvent> livenessJudgmentHandler;

    public LivenessChecker(Predicate<OkEvent> livenessJudgmentHandler) {
        this.livenessJudgmentHandler = livenessJudgmentHandler;
    }

    @Override
    public CheckerResult check(List<Event> events) {
        Visitor visitor = new Visitor(livenessJudgmentHandler);
        events.forEach(event -> event.accept(visitor));
        return ImmutableCheckerResult.builder()
                .valid(visitor.valid())
                .errors(visitor.errors())
                .build();
    }

    private static final class Visitor implements EventVisitor {
        private static final int DUMMY_PROCESS_VALUE = 0;
        private static final String FUNCTION = "liveness-check";

        private final Predicate<OkEvent> livenessJudgmentHandler;

        private boolean seenLivenessEvidence = false;
        private long lastSeenEventTimestamp = Long.MIN_VALUE;

        private Visitor(Predicate<OkEvent> livenessJudgmentHandler) {
            this.livenessJudgmentHandler = livenessJudgmentHandler;
        }

        @Override
        public void visit(InfoEvent event) {
            logEventTimestamp(event.time());
        }

        @Override
        public void visit(InvokeEvent event) {
            logEventTimestamp(event.time());
        }

        @Override
        public void visit(OkEvent event) {
            if (livenessJudgmentHandler.test(event)) {
                seenLivenessEvidence = true;
            }
            logEventTimestamp(event.time());
        }

        @Override
        public void visit(FailEvent event) {
            logEventTimestamp(event.time());
        }

        public boolean valid() {
            return seenLivenessEvidence;
        }

        public List<Event> errors() {
            return valid() ? ImmutableList.of() : ImmutableList.of(
                    ImmutableInfoEvent.builder()
                            .time(lastSeenEventTimestamp)
                            .value("No live requests were actually observed up to this time, which is worrying as to "
                                    + "the "
                                    + "validity of this test.")
                            .function(FUNCTION)
                            .process(DUMMY_PROCESS_VALUE)
                            .build()
            );
        }

        private void logEventTimestamp(long time) {
            lastSeenEventTimestamp = Math.max(lastSeenEventTimestamp, time);
        }
    }
}

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
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import org.immutables.value.Value;

public class LivenessChecker implements Checker {
    private static final int DUMMY_PROCESS_VALUE = 0;
    private static final String FUNCTION = "liveness-check";

    private final Predicate<OkEvent> livenessJudgmentHandler;

    public LivenessChecker(Predicate<OkEvent> livenessJudgmentHandler) {
        this.livenessJudgmentHandler = livenessJudgmentHandler;
    }

    @Override
    public CheckerResult check(List<Event> events) {
        Visitor visitor = new Visitor(livenessJudgmentHandler);

        Optional<Event> firstEvidenceOfLiveness =
                events.stream().filter(event -> event.accept(visitor).live()).findFirst();

        if (firstEvidenceOfLiveness.isPresent()) {
            return ImmutableCheckerResult.builder()
                    .valid(true)
                    .errors(ImmutableList.of())
                    .build();
        }

        return ImmutableCheckerResult.builder()
                .valid(false)
                .errors(createErrorFromEvents(events))
                .build();
    }

    private static List<Event> createErrorFromEvents(List<Event> events) {
        return ImmutableList.of(ImmutableInfoEvent.builder()
                .time(events.stream()
                        .map(Event::time)
                        .max(Comparator.naturalOrder())
                        .orElse(Long.MIN_VALUE))
                .value("No live requests were actually observed up to this time, which is worrying as to "
                        + "the validity of this test.")
                .function(FUNCTION)
                .process(DUMMY_PROCESS_VALUE)
                .build());
    }

    private static final class Visitor implements EventVisitor<LivenessCheckResult> {
        private final Predicate<OkEvent> livenessJudgmentHandler;

        private Visitor(Predicate<OkEvent> livenessJudgmentHandler) {
            this.livenessJudgmentHandler = livenessJudgmentHandler;
        }

        @Override
        public LivenessCheckResult visit(InfoEvent event) {
            return ImmutableLivenessCheckResult.of(false, event);
        }

        @Override
        public LivenessCheckResult visit(InvokeEvent event) {
            return ImmutableLivenessCheckResult.of(false, event);
        }

        @Override
        public LivenessCheckResult visit(OkEvent event) {
            return ImmutableLivenessCheckResult.of(livenessJudgmentHandler.test(event), event);
        }

        @Override
        public LivenessCheckResult visit(FailEvent event) {
            return ImmutableLivenessCheckResult.of(false, event);
        }
    }

    @Value.Immutable
    interface LivenessCheckResult {
        @Value.Parameter
        boolean live();

        @Value.Parameter
        Event event();
    }
}

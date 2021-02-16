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
package com.palantir.atlasdb.jepsen;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.jepsen.events.Checker;
import com.palantir.atlasdb.jepsen.events.Event;
import com.palantir.atlasdb.jepsen.events.EventVisitor;
import com.palantir.atlasdb.jepsen.events.InfoEvent;
import com.palantir.atlasdb.jepsen.events.InvokeEvent;
import com.palantir.atlasdb.jepsen.events.OkEvent;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class NemesisResilienceChecker implements Checker {
    @Override
    public CheckerResult check(List<Event> events) {
        Visitor visitor = new NemesisResilienceChecker.Visitor();
        events.forEach(event -> event.accept(visitor));
        return ImmutableCheckerResult.builder()
                .valid(visitor.valid())
                .errors(visitor.errors())
                .build();
    }

    private static final class Visitor implements EventVisitor<Void> {
        private final List<Event> unsurvivedEvents = new ArrayList<>();
        private final Set<Integer> processesPendingReads = new HashSet<>();

        private Event startEvent;
        private boolean awaitingInvokeOkCycle;

        @Override
        public Void visit(InfoEvent event) {
            if (isNemesisEvent(event)) {
                if (isStartEvent(event)) {
                    startAwaitingInvokeOkCycles(event);
                } else if (isStopEvent(event) && awaitingInvokeOkCycle) {
                    addUnsurvivedEvents(event);
                }
            }
            return null;
        }

        @Override
        public Void visit(InvokeEvent event) {
            if (awaitingInvokeOkCycle) {
                processesPendingReads.add(event.process());
            }
            return null;
        }

        @Override
        public Void visit(OkEvent event) {
            if (awaitingInvokeOkCycle && processesPendingReads.contains(event.process())) {
                awaitingInvokeOkCycle = false;
            }
            return null;
        }

        public boolean valid() {
            return unsurvivedEvents.isEmpty();
        }

        public List<Event> errors() {
            return ImmutableList.copyOf(unsurvivedEvents);
        }

        private static boolean isNemesisEvent(InfoEvent event) {
            return event.process() == JepsenConstants.NEMESIS_PROCESS;
        }

        private static boolean isStartEvent(InfoEvent event) {
            return Objects.equals(event.function(), JepsenConstants.START_FUNCTION);
        }

        private static boolean isStopEvent(InfoEvent event) {
            return Objects.equals(event.function(), JepsenConstants.STOP_FUNCTION);
        }

        private void startAwaitingInvokeOkCycles(InfoEvent event) {
            startEvent = event;
            awaitingInvokeOkCycle = true;
            processesPendingReads.clear();
        }

        private void addUnsurvivedEvents(InfoEvent event) {
            unsurvivedEvents.add(startEvent);
            unsurvivedEvents.add(event);
            awaitingInvokeOkCycle = false;
        }
    }
}

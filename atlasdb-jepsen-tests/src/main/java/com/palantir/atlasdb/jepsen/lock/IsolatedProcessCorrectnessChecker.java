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
package com.palantir.atlasdb.jepsen.lock;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.jepsen.CheckerResult;
import com.palantir.atlasdb.jepsen.ImmutableCheckerResult;
import com.palantir.atlasdb.jepsen.events.Checker;
import com.palantir.atlasdb.jepsen.events.Event;
import com.palantir.atlasdb.jepsen.events.EventVisitor;
import com.palantir.atlasdb.jepsen.events.InvokeEvent;
import com.palantir.atlasdb.jepsen.events.OkEvent;
import com.palantir.atlasdb.jepsen.events.RequestType;
import com.palantir.atlasdb.jepsen.utils.EventUtils;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This checker verifies that the sequence of events is correct for each process in isolation. Since we know that no
 * process makes a new request before getting a reply from the old one, we only need to consider the OkEvents.
 */
public class IsolatedProcessCorrectnessChecker implements Checker {
    @Override
    public CheckerResult check(List<Event> events) {
        Visitor visitor = new Visitor();
        events.forEach(event -> event.accept(visitor));
        return ImmutableCheckerResult.builder()
                .valid(visitor.valid())
                .errors(visitor.errors())
                .build();
    }

    private static final class Visitor implements EventVisitor {
        private final Map<Integer, InvokeEvent> pendingForProcess = new HashMap<>();
        private final Map<Integer, OkEvent> lastOkEvent = new HashMap<>();
        private final Set<Integer> refreshAllowed = new HashSet<>();

        private final List<Event> errors = new ArrayList<>();

        @Override
        public void visit(InvokeEvent event) {
            pendingForProcess.put(event.process(), event);
        }

        /**
         * LOCK:
         *  - SUCCESS: held the lock at some point between request and reply.
         *  - FAILURE: the lock was held by someone else at some point between request and reply.
         *
         * UNLOCK:
         *  - SUCCESS: was holding the lock, now does not hold it.
         *  - FAILURE: was not holding the lock.
         *
         * REFRESH:
         *  - SUCCESS: was holding the lock, still possibly does.
         *  - FAILURE: was not holding the lock.
         */
        @Override
        public void visit(OkEvent event) {
            int currentProcess = event.process();

            switch (event.function()) {
                case RequestType.LOCK:
                    if (EventUtils.isFailure(event)) {
                        refreshAllowed.remove(currentProcess);
                    } else {
                        refreshAllowed.add(currentProcess);
                    }
                    break;
                case RequestType.REFRESH:
                    if (EventUtils.isFailure(event)) {
                        refreshAllowed.remove(currentProcess);
                    }
                    verifyRefreshAllowed(event, currentProcess);
                    break;
                case RequestType.UNLOCK:
                    verifyRefreshAllowed(event, currentProcess);
                    refreshAllowed.remove(currentProcess);
                    break;
                default:
                    throw new SafeIllegalStateException("Not an OkEvent type supported by this checker!");
            }
            lastOkEvent.put(currentProcess, event);
        }

        private void verifyRefreshAllowed(OkEvent event, Integer process) {
            if (!EventUtils.isFailure(event) && !refreshAllowed.contains(process)) {
                addEventsToErrors(event, process);
                refreshAllowed.add(process);
            }
        }

        private void addEventsToErrors(OkEvent event, Integer process) {
            Event previousEvent;
            if (lastOkEvent.containsKey(process)) {
                previousEvent = lastOkEvent.get(process);
            } else {
                previousEvent = pendingForProcess.get(event.process());
            }
            errors.add(previousEvent);
            errors.add(event);
        }

        public boolean valid() {
            return errors.isEmpty();
        }

        public List<Event> errors() {
            return ImmutableList.copyOf(errors);
        }
    }
}

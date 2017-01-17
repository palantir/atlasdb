/**
 * Copyright 2016 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.jepsen.lock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.jepsen.CheckerResult;
import com.palantir.atlasdb.jepsen.ImmutableCheckerResult;
import com.palantir.atlasdb.jepsen.events.Checker;
import com.palantir.atlasdb.jepsen.events.Event;
import com.palantir.atlasdb.jepsen.events.EventVisitor;
import com.palantir.atlasdb.jepsen.events.FailEvent;
import com.palantir.atlasdb.jepsen.events.InfoEvent;
import com.palantir.atlasdb.jepsen.events.InvokeEvent;
import com.palantir.atlasdb.jepsen.events.OkEvent;
import com.palantir.atlasdb.jepsen.events.RequestType;

import com.palantir.util.Pair;

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

    private static class Visitor implements EventVisitor {
        private final Map<Pair<Integer, String>, Boolean> refreshAllowed = new HashMap<>();
        private final Map<Pair<Integer, String>, OkEvent> lastEvent = new HashMap<>();
        private final List<Event> errors = new ArrayList<>();
        private final Map<Integer, String> resourceName = new HashMap<>();

        @Override
        public void visit(InfoEvent event) {
        }

        @Override
        public void visit(InvokeEvent event) {
            resourceName.put(event.process(), event.value());
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
            String lockName = resourceName.get(currentProcess);
            Pair processLock = new Pair(currentProcess, lockName);

            if (!refreshAllowed.containsKey(processLock)) {
                refreshAllowed.put(processLock, false);
                lastEvent.put(processLock, event);
            }

            switch (event.function()) {
                case RequestType.LOCK:
                    if (event.isFailure()) {
                        refreshAllowed.put(processLock, false);
                    }
                    if (event.isSuccessful()) {
                        refreshAllowed.put(processLock, true);
                    }
                    break;
                case RequestType.REFRESH:
                    if (event.isFailure()) {
                        refreshAllowed.put(processLock, false);
                    }
                    if (event.isSuccessful()) {
                        if (!refreshAllowed.get(processLock)) {
                            errors.add(lastEvent.get(processLock));
                            errors.add(event);
                            refreshAllowed.put(processLock, true);
                        }
                    }
                    break;
                case RequestType.UNLOCK:
                    if (event.isSuccessful()) {
                        if (!refreshAllowed.get(processLock)) {
                            errors.add(lastEvent.get(processLock));
                            errors.add(event);
                        }
                    }
                    refreshAllowed.put(processLock, false);
                    break;
                default: break;
            }
            lastEvent.put(processLock, event);
        }

        @Override
        public void visit(FailEvent event) {
        }

        public boolean valid() {
            return errors.isEmpty();
        }

        public List<Event> errors() {
            return ImmutableList.copyOf(errors);
        }
    }
}

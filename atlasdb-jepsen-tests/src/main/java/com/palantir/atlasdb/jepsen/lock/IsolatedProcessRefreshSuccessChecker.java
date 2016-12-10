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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import com.palantir.atlasdb.jepsen.CheckerResult;
import com.palantir.atlasdb.jepsen.ImmutableCheckerResult;
import com.palantir.atlasdb.jepsen.events.Checker;
import com.palantir.atlasdb.jepsen.events.Event;
import com.palantir.atlasdb.jepsen.events.EventVisitor;
import com.palantir.atlasdb.jepsen.events.FailEvent;
import com.palantir.atlasdb.jepsen.events.InfoEvent;
import com.palantir.atlasdb.jepsen.events.InvokeEvent;
import com.palantir.atlasdb.jepsen.events.OkEvent;
import com.palantir.util.Pair;

/**
 * This checker verifies that a refresh is only successful when that is allowed, considering events of each host
 * separately. Since we know that no process makes a new request before getting a reply from the old one, this is
 * much simpler -- we only need to consider the OkEvents.
 */
public class IsolatedProcessRefreshSuccessChecker implements Checker {
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
        private final Map<Integer, Boolean> refreshAllowed = new HashMap<>();
        private final Map<Integer, OkEvent> lastEvent = new HashMap<>();
        private final Set<Integer> processes = new HashSet<>();

        private final List<Event> errors = new ArrayList<>();

        @Override
        public void visit(InfoEvent event) {
        }

        @Override
        public void visit(InvokeEvent event) {
        }

        /**
         * Successful lock means that future refreshes are allowed, successful unlock means refreshes are forbidden,
         * unsuccessful refresh means the same.
         *
         * I assume unlock should fail if you do not have a lock.
         *
         * I assume lock is allowed to fail randomly, and unlock is allowed to fail randomly.
         */
        @Override
        public void visit(OkEvent event) {
            int currentProcess = event.process();
            if (!processes.contains(currentProcess)){
                processes.add(currentProcess);
                refreshAllowed.put(currentProcess, false);
                lastEvent.put(currentProcess, event);
            }
            switch (event.requestType()){
                case LOCK:
                    if (event.value() == OkEvent.SUCCESS) {
                        refreshAllowed.put(currentProcess, true);
                        lastEvent.put(currentProcess, event);
                    }
                    break;
                case REFRESH:
                    if (event.value() == OkEvent.FAILURE) {
                        refreshAllowed.put(currentProcess, false);
                        lastEvent.put(currentProcess, event);
                    }
                    if (event.value() == OkEvent.SUCCESS){
                        if (!refreshAllowed.get(currentProcess)) {
                            errors.add(lastEvent.get(currentProcess));
                            errors.add(event);
                            refreshAllowed.put(currentProcess, true);
                        }
                    }
                    lastEvent.put(currentProcess, event);
                    break;
                case UNLOCK:
                    if (event.value() == OkEvent.SUCCESS) {
                        if (!refreshAllowed.get(currentProcess)) {
                            errors.add(lastEvent.get(currentProcess));
                            errors.add(event);
                        }
                        refreshAllowed.put(currentProcess, false);
                        lastEvent.put(currentProcess, event);
                    }
            }
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

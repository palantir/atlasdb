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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.palantir.atlasdb.jepsen.events.RequestType;

import com.palantir.atlasdb.jepsen.utils.EventUtils;
import com.palantir.util.Pair;

/**
 * This checker verifies that refreshes of locks do not cause two processes to simultaneously hold the same lock.
 * We assume that the events of each process in isolation are correct.
 */
public class RefreshCorrectnessChecker implements Checker {

    private static final Logger log = LoggerFactory.getLogger(RefreshCorrectnessChecker.class);

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
        private final Map<Pair<Integer, String>, InvokeEvent> pendingForProcessAndLock = new HashMap<>();
        private final Map<Pair<Integer, String>, Event> lastHeldLock = new HashMap<>();
        private final Map<String, TreeRangeSet<Long>> locksHeld = new HashMap<>();
        private final ArrayList<String> allLockNames = new ArrayList<>();
        private final List<Event> errors = new ArrayList<>();
        private final Map<Integer, String> resourceName = new HashMap<>();

        @Override
        public void visit(InfoEvent event) {
        }

        @Override
        public void visit(InvokeEvent event) {
            Integer process = event.process();
            String lockName = event.value();
            resourceName.put(process, lockName);
            pendingForProcessAndLock.put(new Pair(process, lockName), event);

            if (!allLockNames.contains(lockName)) {
                allLockNames.add(lockName);
                locksHeld.put(lockName, TreeRangeSet.create());
            }
        }

        @Override
        public void visit(OkEvent event) {
            Integer process = event.process();
            String lockName = resourceName.get(process);
            Pair processLock = new Pair(process, lockName);
            InvokeEvent invokeEvent = pendingForProcessAndLock.get(processLock);

            switch (event.function()) {
                /**
                 * Successful LOCK:
                 * Remember the new value for the most recent successful lock
                 */
                case RequestType.LOCK:
                    if (EventUtils.isSuccessful(event)) {
                        lastHeldLock.put(processLock, event);
                    }
                    break;
                /**
                 * Successful REFRESH/UNLOCK:
                 * Add the new interval [a, b) to the set of known locks, where
                 * a is the OkEvent.time() of the last successful lock or the InvokeEvent.time() of refresh, and
                 * b is the InvokeEvent.time() of the current request, if and only if b > a.
                 * Also verify that the whole interval was free. Unlock can be treated as refresh, as the correctness
                 * of their mutual interaction is verified by IsolatedProcessCorrectnessChecker
                 */
                case RequestType.REFRESH:
                case RequestType.UNLOCK:
                    if (EventUtils.isSuccessful(event) && lastHeldLock.containsKey(processLock)) {
                        long lastLockTime = lastHeldLock.get(processLock).time();
                        if (lastLockTime < invokeEvent.time()) {
                            Range<Long> newRange = Range.closedOpen(lastLockTime, invokeEvent.time());
                            if (!locksHeld.get(lockName).complement().encloses(newRange)) {
                                log.error("A {} request for lock {} by process {} invoked at time {} was granted at "
                                        + "time {}, but another process was granted the lock between {} and {} "
                                        + "(last known time the lock was held by {})",
                                        invokeEvent.function(), invokeEvent.value(), invokeEvent.process(),
                                        invokeEvent.time(), event.time(), lastLockTime, invokeEvent.time(),
                                        invokeEvent.process());
                                errors.add(invokeEvent);
                                errors.add(event);
                            }
                            locksHeld.get(lockName).add(newRange);
                            lastHeldLock.put(processLock, invokeEvent);
                        }
                    }
                    break;
                default : break;
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

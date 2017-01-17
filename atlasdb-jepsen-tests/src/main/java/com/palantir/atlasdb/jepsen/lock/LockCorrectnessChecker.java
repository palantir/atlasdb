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

import com.palantir.util.Pair;

/**
 * Checker verifying that whenever a lock is granted, there was a time point between the request and the
 * acknowledge when the lock was actually free to be granted. This is tricky due to the existence of refreshes
 * and the uncertainty of the exact times because of the latency between requests and replies.
 *
 * A successful refresh means the lock was held in the (open) interval
 * (a, b), where
 * a is the OkEvent.time() of the last successful lock,
 * b is the InvokeEvent.time() of the refresh,
 * <i>>assuming</i> a successful refresh guarantees holding the lock since the last time the lock was granted. This will
 * be verified by another checker.
 *
 * A successful unlock can be treated the same as a refresh, except that it has implications for further refreshes
 * and unlocks (that we will check by other checkers).
 *
 * A successful lock means the lock was held <i>at some point</i> in the (closed) interval
 * [a, b], where
 * a is the InvokeEvent.time() of the lock request, and
 * b is its OkEvent.time()
 *
 * We mantain a set L of all intervals where we know there was a lock, and a separate list U of all intervals where
 * there was a lock at some point. In the latter list, we purposefully do not merge intervals, to ensure no information
 * is lost.
 *
 * To verify locks were correctly granted, it is necessary and sufficient to verify that none of the uncertain
 * intervals in U are fully covered by the intervals in the set of lock intervals L.
 */
public class LockCorrectnessChecker implements Checker {

    private static final Logger log = LoggerFactory.getLogger(LockCorrectnessChecker.class);

    @Override
    public CheckerResult check(List<Event> events) {
        Visitor visitor = new Visitor();
        events.forEach(event -> event.accept(visitor));
        visitor.verifyLockCorrectness();
        return ImmutableCheckerResult.builder()
                .valid(visitor.valid())
                .errors(visitor.errors())
                .build();
    }

    private static class Visitor implements EventVisitor {
        private final Map<Pair<Integer, String>, InvokeEvent> pendingForProcessAndLock = new HashMap<>();
        private final Map<Pair<Integer, String>, OkEvent> lastHeldLock = new HashMap<>();

        private final Map<String, TreeRangeSet<Long>> locksHeld = new HashMap<>();
        private final Map<String, List<Pair<InvokeEvent, OkEvent>>> locksAtSomePoint = new HashMap<>();

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
                locksAtSomePoint.put(lockName, new ArrayList<>());
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
                 * 1) Add a new uncertain interval to verify at the end,
                 * 2) Remember the new value for the most recent successful lock
                 */
                case RequestType.LOCK:
                    if (event.isSuccessful()) {
                        locksAtSomePoint.get(lockName).add(new Pair(invokeEvent, event));
                        lastHeldLock.put(processLock, event);
                    }
                    break;
                /**
                 * Successful REFRESH/UNLOCK:
                 * Add the new interval (a, b) to the set of known locks, possibly absorbing a previously
                 * existing interval (a, b'). In this checker, we are assuming correctness of refreshes and unlocks
                 * which allows for somewhat simpler logic, and makes it unnecessary to verify the failed instances.
                 */
                case RequestType.REFRESH:
                case RequestType.UNLOCK:
                    if (event.isSuccessful() && lastHeldLock.containsKey(processLock)) {
                        long lastLockTime = lastHeldLock.get(processLock).time();
                        if (lastLockTime < invokeEvent.time()) {
                            locksHeld.get(lockName).add(
                                    Range.open(lastLockTime, invokeEvent.time()));
                        }
                    }
                    break;
                default:
                    break;
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

        private void verifyLockCorrectness() {
            locksAtSomePoint.entrySet().forEach(entry ->
                    entry.getValue().forEach(eventPair -> {
                        String lockName = entry.getKey();
                        InvokeEvent invokeEvent = eventPair.getLhSide();
                        OkEvent okEvent = eventPair.getRhSide();
                        if (intervalCovered(lockName, invokeEvent, okEvent)) {
                            log.error("Lock {} granted to process {} between {} and {}, but lock was already held by "
                                    + "another process.",
                                    lockName, invokeEvent.process(), invokeEvent.time(), okEvent.time());
                            errors.add(invokeEvent);
                            errors.add(okEvent);
                        }
                    }));
        }

        private boolean intervalCovered(String lockName, InvokeEvent invokeEvent, OkEvent okEvent) {
            Range<Long> interval = Range.closed(invokeEvent.time(), okEvent.time());
            return locksHeld.get(lockName).encloses(interval);
        }

    }
}

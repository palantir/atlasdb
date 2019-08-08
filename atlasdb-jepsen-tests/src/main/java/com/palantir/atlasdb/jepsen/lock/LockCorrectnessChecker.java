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
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
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
import com.palantir.util.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checker verifying that whenever a lock is granted, there was a time point between the request and the
 * acknowledge when the lock was actually free to be granted. This is tricky due to the existence of refreshes
 * and the uncertainty of the exact times because of the latency between requests and replies.
 *
 * A successful refresh means the lock was continuously held in the (open) interval
 * (a, b), where
 * a is the OkEvent.time() of the last successful lock,
 * b is the InvokeEvent.time() of the refresh,
 * <i>assuming</i> a successful refresh guarantees holding the lock since the last time the lock was granted. This will
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
 * We mantain a map locksHeld mapping each lock name to the set of all intervals where we know the lock was granted to
 * a process, and a separate list map locksAtSomePoint tracking all intervals during which the lock was granted at some
 * point. Note that in the latter list, we purposefully do not merge intervals, to ensure no loss of information.
 *
 * To verify locks were correctly granted, it is necessary (though not sufficient -- we just don't have enough
 * information) to verify that, for each lockName, none of the uncertain intervals in locksAtSomePoint.get(lockName)
 * are fully covered by the intervals in the set of lock intervals locksHeld.get(lockName).
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
        private final Map<Integer, InvokeEvent> pendingForProcess = new HashMap<>();
        private final Map<Integer, OkEvent> lastHeldLock = new HashMap<>();

        private final TreeRangeSet<Long> locksHeld = TreeRangeSet.create();
        private final List<Pair<InvokeEvent, OkEvent>> locksAtSomePoint = new ArrayList<>();

        private final List<Event> errors = new ArrayList<>();

        private String lockName = null;

        @Override
        public void visit(InvokeEvent event) {
            int process = event.process();
            pendingForProcess.put(process, event);
            this.lockName = event.value();
        }

        @Override
        public void visit(OkEvent event) {
            if (EventUtils.isFailure(event)) {
                return;
            }

            int process = event.process();
            InvokeEvent invokeEvent = pendingForProcess.get(process);

            switch (event.function()) {
                /*
                 * Successful LOCK:
                 * 1) Add a new uncertain interval to verify at the end,
                 * 2) Remember the new value for the most recent successful lock
                 */
                case RequestType.LOCK:
                    locksAtSomePoint.add(new Pair(invokeEvent, event));
                    lastHeldLock.put(process, event);
                    break;
                /*
                 * Successful REFRESH/UNLOCK:
                 * Add the new interval (a, b) to the set of known locks, possibly absorbing a previously
                 * existing interval (a, b'). In this checker, we are assuming correctness of refreshes and unlocks
                 * which allows for somewhat simpler logic, and makes it unnecessary to verify the failed instances.
                 */
                case RequestType.REFRESH:
                case RequestType.UNLOCK:
                    if (lastHeldLock.containsKey(process)) {
                        long lastLockTime = lastHeldLock.get(process).time();
                        if (lastLockTime < invokeEvent.time()) {
                            locksHeld.add(Range.open(lastLockTime, invokeEvent.time()));
                        }
                    }
                    break;
                default: throw new SafeIllegalStateException("Not an OkEvent type supported by this checker!");
            }
        }

        private void verifyLockCorrectness() {
            for (Pair<InvokeEvent, OkEvent> eventPair : locksAtSomePoint) {
                InvokeEvent invokeEvent = eventPair.getLhSide();
                OkEvent okEvent = eventPair.getRhSide();
                if (intervalCovered(invokeEvent, okEvent)) {
                    log.error("Lock {} granted to process {} between {} and {}, but lock was already held by "
                                    + "another process.",
                            lockName, invokeEvent.process(), invokeEvent.time(), okEvent.time());
                    errors.add(invokeEvent);
                    errors.add(okEvent);
                }
            }
        }

        private boolean intervalCovered(InvokeEvent invokeEvent, OkEvent okEvent) {
            Range<Long> interval = Range.closed(invokeEvent.time(), okEvent.time());
            return locksHeld.encloses(interval);
        }

        public boolean valid() {
            return errors.isEmpty();
        }

        public List<Event> errors() {
            return ImmutableList.copyOf(errors);
        }

    }
}

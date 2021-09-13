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
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This checker verifies that refreshes of locks do not cause two processes to simultaneously hold the same lock.
 * We assume that the events of each process in isolation are correct.
 */
public class RefreshCorrectnessChecker implements Checker {

    private static final SafeLogger log = SafeLoggerFactory.get(RefreshCorrectnessChecker.class);

    @Override
    public CheckerResult check(List<Event> events) {
        Visitor visitor = new Visitor();
        events.forEach(event -> event.accept(visitor));
        return ImmutableCheckerResult.builder()
                .valid(visitor.valid())
                .errors(visitor.errors())
                .build();
    }

    private static final class Visitor implements EventVisitor<Void> {
        private final Map<Integer, InvokeEvent> pendingForProcess = new HashMap<>();
        private final Map<Integer, Event> lastHeldLock = new HashMap<>();

        private final TreeRangeSet<Long> locksHeld = TreeRangeSet.create();

        private final List<Event> errors = new ArrayList<>();

        @Override
        public Void visit(InvokeEvent event) {
            int process = event.process();
            pendingForProcess.put(process, event);
            return null;
        }

        @Override
        public Void visit(OkEvent event) {
            if (EventUtils.isFailure(event)) {
                return null;
            }

            int process = event.process();
            InvokeEvent invokeEvent = pendingForProcess.get(process);

            switch (event.function()) {
                    /*
                     * Successful LOCK:
                     * Remember the new value for the most recent successful lock
                     */
                case RequestType.LOCK:
                    lastHeldLock.put(process, event);
                    break;
                    /*
                     * Successful REFRESH/UNLOCK:
                     * Add the new interval [a, b) to the set of known locks, where
                     *
                     * a is the last time for which we know the lock was held, the greater value of:
                     *      - the InvokeEvent.time() of a successful refresh, or
                     *      - the OkEvent.time() of a successful lock.
                     * b is the InvokeEvent.time() of the current request, if and only if b > a.
                     *
                     * Note that including a is an overapproximation of the size of the interval, as in the case where
                     * a is the OkEvent.time() of a lock, we should instead take (a, b). This is, however, OK because in
                     * this checker we only look for intersecting intervals and all intervals are open from the right so
                     * including a does not affect the result.
                     *
                     * Also verify that the whole interval was free. Unlock can be treated as refresh, as the
                     * correctness of their mutual interaction is verified by IsolatedProcessCorrectnessChecker
                     */
                case RequestType.REFRESH:
                case RequestType.UNLOCK:
                    if (lastHeldLock.containsKey(process)) {
                        long lastLockTime = lastHeldLock.get(process).time();
                        if (lastLockTime < invokeEvent.time()) {
                            Range<Long> newRange = Range.closedOpen(lastLockTime, invokeEvent.time());
                            if (!locksHeld.subRangeSet(newRange).isEmpty()) {
                                log.error(
                                        "A {} request for lock {} by process {} invoked at time {} was granted at "
                                                + "time {}, but another process was granted the lock between {} and {} "
                                                + "(last known time the lock was held by {})",
                                        UnsafeArg.of("function", invokeEvent.function()),
                                        UnsafeArg.of("value", invokeEvent.value()),
                                        SafeArg.of("process", invokeEvent.process()),
                                        SafeArg.of("invokeTime", invokeEvent.time()),
                                        SafeArg.of("eventTime", event.time()),
                                        SafeArg.of("lastLockTime", lastLockTime),
                                        SafeArg.of("invokeTime2", invokeEvent.time()),
                                        SafeArg.of("process2", invokeEvent.process()));
                                errors.add(invokeEvent);
                                errors.add(event);
                            }
                            locksHeld.add(newRange);
                            lastHeldLock.put(process, invokeEvent);
                        }
                    }
                    break;
                default:
                    throw new SafeIllegalStateException("Not an OkEvent type supported by this checker!");
            }
            return null;
        }

        public boolean valid() {
            return errors.isEmpty();
        }

        public List<Event> errors() {
            return ImmutableList.copyOf(errors);
        }
    }
}

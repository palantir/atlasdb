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

package com.palantir.atlasdb.jepsen.timestamp;

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

/**
 * Verifies that the number of timestamp requests that succeeded was at least 1.
 */
public class TimestampLivenessChecker implements Checker {

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
        private static final int DUMMY_PROCESS_VALUE = 0;
        private static final String READ_OPERATION = "read-operation";

        private boolean seenOkEvent = false;
        private long lastSeenTimestamp = Long.MIN_VALUE;

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
            seenOkEvent = true;
            logEventTimestamp(event.time());
        }

        @Override
        public void visit(FailEvent event) {
            logEventTimestamp(event.time());
        }

        public boolean valid() {
            return seenOkEvent;
        }

        public List<Event> errors() {
            return valid() ? ImmutableList.of() : ImmutableList.of(
                    ImmutableInfoEvent.builder()
                    .time(lastSeenTimestamp)
                    .value("No timestamps were actually retrieved up to this time, which is worrying as to the "
                            + "validity of this test.")
                    .function(READ_OPERATION)
                    .process(DUMMY_PROCESS_VALUE)
                    .build()
            );
        }

        private void logEventTimestamp(long time) {
            lastSeenTimestamp = Math.max(lastSeenTimestamp, time);
        }
    }
}

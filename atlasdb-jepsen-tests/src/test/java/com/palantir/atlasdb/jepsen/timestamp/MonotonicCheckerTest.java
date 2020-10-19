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
package com.palantir.atlasdb.jepsen.timestamp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.jepsen.CheckerResult;
import com.palantir.atlasdb.jepsen.events.Event;
import com.palantir.atlasdb.jepsen.utils.CheckerTestUtils;
import com.palantir.atlasdb.jepsen.utils.TestEventUtils;
import org.junit.Test;

public class MonotonicCheckerTest {
    private static final Long ZERO_TIME = 0L;
    private static final int PROCESS_0 = 0;
    private static final int PROCESS_1 = 1;
    private static final String INT_MAX_PLUS_ONE = String.valueOf(1L + Integer.MAX_VALUE);
    private static final String NOOP = "noop";

    @Test
    public void shouldSucceedOnNoEvents() {
        CheckerResult result = runMonotonicChecker();

        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    @Test
    public void shouldFailOnDecreasingValueEvents() {
        Event event1 = TestEventUtils.timestampOk(ZERO_TIME, PROCESS_0, "1");
        Event event2 = TestEventUtils.timestampOk(ZERO_TIME, PROCESS_0, "0");

        CheckerResult result = runMonotonicChecker(event1, event2);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(event1, event2);
    }

    @Test
    public void shouldFailOnEqualEntries() {
        Event event1 = TestEventUtils.timestampOk(ZERO_TIME, PROCESS_0, "0");
        Event event2 = TestEventUtils.timestampOk(ZERO_TIME, PROCESS_0, "0");

        CheckerResult result = runMonotonicChecker(event1, event2);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(event1, event2);
    }

    @Test
    public void shouldSucceedOnTwoProcessesOverlapping() {
        Event event1 = TestEventUtils.timestampOk(ZERO_TIME, PROCESS_0, "1");
        Event event2 = TestEventUtils.timestampOk(ZERO_TIME, PROCESS_1, "2");
        Event event3 = TestEventUtils.timestampOk(ZERO_TIME, PROCESS_0, "4");
        Event event4 = TestEventUtils.timestampOk(ZERO_TIME, PROCESS_1, "3");

        CheckerTestUtils.assertNoErrors(MonotonicChecker::new,
                event1, event2, event3, event4);
    }

    @Test
    public void shouldThrowIfOkEventHasNonIntegerValue() {
        Event event1 = TestEventUtils.timestampOk(ZERO_TIME, PROCESS_0, NOOP);
        Event event2 = TestEventUtils.timestampOk(ZERO_TIME, PROCESS_0, NOOP);

        assertThatThrownBy(() -> runMonotonicChecker(event1, event2))
                .isInstanceOf(NumberFormatException.class);
    }

    @Test
    public void shouldParseLongValues() {
        Event event1 = TestEventUtils.timestampOk(ZERO_TIME, PROCESS_0, INT_MAX_PLUS_ONE);

        CheckerTestUtils.assertNoErrors(MonotonicChecker::new,
                event1);
    }

    private static CheckerResult runMonotonicChecker(Event... events) {
        MonotonicChecker monotonicChecker = new MonotonicChecker();
        return monotonicChecker.check(ImmutableList.copyOf(events));
    }
}

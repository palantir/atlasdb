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
package com.palantir.atlasdb.jepsen;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.jepsen.events.Event;
import com.palantir.atlasdb.jepsen.events.ImmutableOkEvent;

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
        Event event1 = ImmutableOkEvent.builder()
                .time(ZERO_TIME)
                .process(PROCESS_0)
                .value("1")
                .build();
        Event event2 = ImmutableOkEvent.builder()
                .time(ZERO_TIME)
                .process(PROCESS_0)
                .value("0")
                .build();

        CheckerResult result = runMonotonicChecker(event1, event2);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(event1, event2);
    }

    @Test
    public void shouldFailOnEqualEntries() {
        Event event1 = ImmutableOkEvent.builder()
                .time(ZERO_TIME)
                .process(PROCESS_0)
                .value("0")
                .build();
        Event event2 = ImmutableOkEvent.builder()
                .time(ZERO_TIME)
                .process(PROCESS_0)
                .value("0")
                .build();

        CheckerResult result = runMonotonicChecker(event1, event2);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(event1, event2);
    }

    @Test
    public void shouldSucceedOnTwoProcessesOverlapping() {
        Event event1 = ImmutableOkEvent.builder()
                .time(ZERO_TIME)
                .process(PROCESS_0)
                .value("1")
                .build();
        Event event2 = ImmutableOkEvent.builder()
                .time(ZERO_TIME)
                .process(PROCESS_1)
                .value("2")
                .build();
        Event event3 = ImmutableOkEvent.builder()
                .time(ZERO_TIME)
                .process(PROCESS_0)
                .value("4")
                .build();
        Event event4 = ImmutableOkEvent.builder()
                .time(ZERO_TIME)
                .process(PROCESS_1)
                .value("3")
                .build();

        CheckerTestUtils.assertNoErrors(MonotonicChecker::new,
                event1, event2, event3, event4);
    }

    @Test
    public void shouldThrowIfOkEventHasNonIntegerValue() {
        Event event1 = ImmutableOkEvent.builder()
                .time(ZERO_TIME)
                .process(PROCESS_0)
                .value(NOOP)
                .build();
        Event event2 = ImmutableOkEvent.builder()
                .time(ZERO_TIME)
                .process(PROCESS_0)
                .value(NOOP)
                .build();

        assertThatThrownBy(() -> runMonotonicChecker(event1, event2))
                .isInstanceOf(NumberFormatException.class);
    }

    @Test
    public void shouldParseLongValues() {
        Event event1 = ImmutableOkEvent.builder()
                .time(ZERO_TIME)
                .process(PROCESS_0)
                .value(INT_MAX_PLUS_ONE)
                .build();

        CheckerTestUtils.assertNoErrors(MonotonicChecker::new,
                event1);
    }

    private static CheckerResult runMonotonicChecker(Event... events) {
        MonotonicChecker monotonicChecker = new MonotonicChecker();
        return monotonicChecker.check(ImmutableList.copyOf(events));
    }
}

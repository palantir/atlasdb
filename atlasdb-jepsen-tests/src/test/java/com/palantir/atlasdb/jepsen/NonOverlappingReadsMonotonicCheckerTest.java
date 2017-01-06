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
import com.palantir.atlasdb.jepsen.events.ImmutableFailEvent;
import com.palantir.atlasdb.jepsen.events.ImmutableInvokeEvent;
import com.palantir.atlasdb.jepsen.events.ImmutableOkEvent;

public class NonOverlappingReadsMonotonicCheckerTest {
    private static final int PROCESS_0 = 0;
    private static final int PROCESS_1 = 1;
    private static final Long INT_MAX_PLUS_ONE = 1L + Integer.MAX_VALUE;
    private static final Long INT_MAX_PLUS_TWO = 2L + Integer.MAX_VALUE;

    @Test
    public void shouldSucceedOnNoEvents() {
        CheckerResult result = runChecker();

        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    @Test
    public void shouldFailOnDecreasingConfirmedReadsOnOneProcess() {
        long time = 0;
        Event event1 = createInvokeEvent(time++, PROCESS_0);
        Event event2 = createOkEvent(time++, PROCESS_0, "1");
        Event event3 = createInvokeEvent(time++, PROCESS_0);
        Event event4 = createOkEvent(time++, PROCESS_0, "0");

        CheckerResult result = runChecker(event1, event2, event3, event4);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(event2, event3, event4);
    }

    @Test
    public void shouldFailOnDecreasingConfirmedReadsAcrossTwoProcesses() {
        long time = 0;
        Event event1 = createInvokeEvent(time++, PROCESS_0);
        Event event2 = createOkEvent(time++, PROCESS_0, "1");
        Event event3 = createInvokeEvent(time++, PROCESS_1);
        Event event4 = createOkEvent(time++, PROCESS_1, "0");

        CheckerResult result = runChecker(event1, event2, event3, event4);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(event2, event3, event4);
    }

    @Test
    public void shouldFailOnEqualConfirmedReads() {
        long time = 0;
        Event event1 = createInvokeEvent(time++, PROCESS_0);
        Event event2 = createOkEvent(time++, PROCESS_0, "0");
        Event event3 = createInvokeEvent(time++, PROCESS_0);
        Event event4 = createOkEvent(time++, PROCESS_0, "0");

        CheckerResult result = runChecker(event1, event2, event3, event4);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(event2, event3, event4);
    }

    @Test
    public void shouldSucceedOnOverlappingReadsOnTwoProcesses() {
        long time = 0;
        Event event1 = createInvokeEvent(time++, PROCESS_0);
        Event event2 = createInvokeEvent(time++, PROCESS_1);
        Event event3 = createOkEvent(time++, PROCESS_0, "1");
        Event event4 = createOkEvent(time++, PROCESS_1, "0");

        CheckerTestUtils.assertNoErrors(NonOverlappingReadsMonotonicChecker::new,
                event1, event2, event3, event4);
    }

    @Test
    public void failureShouldNegateInvokeOnOneProcess() {
        long time = 0;
        Event event1 = createInvokeEvent(time++, PROCESS_0);
        Event event2 = createFailEvent(time++, PROCESS_0);
        Event event3 = createInvokeEvent(time++, PROCESS_0);
        Event event4 = createOkEvent(time++, PROCESS_0, "1");

        CheckerTestUtils.assertNoErrors(NonOverlappingReadsMonotonicChecker::new,
                event1, event2, event3, event4);
    }

    @Test
    public void shouldIgnoreFailureOnOtherProcess() {
        long time = 0;
        Event event1 = createInvokeEvent(time++, PROCESS_0);
        Event event2 = createFailEvent(time++, PROCESS_0);
        Event event3 = createInvokeEvent(time++, PROCESS_1);
        Event event4 = createOkEvent(time++, PROCESS_1, "1");

        CheckerTestUtils.assertNoErrors(NonOverlappingReadsMonotonicChecker::new,
                event1, event2, event3, event4);
    }

    @Test
    public void shouldIgnoreOverlappingFailure() {
        long time = 0;
        Event event1 = createInvokeEvent(time++, PROCESS_0);
        Event event3 = createInvokeEvent(time++, PROCESS_1);
        Event event2 = createFailEvent(time++, PROCESS_0);
        Event event4 = createOkEvent(time++, PROCESS_1, "1");

        CheckerTestUtils.assertNoErrors(NonOverlappingReadsMonotonicChecker::new,
                event1, event2, event3, event4);
    }

    @Test
    public void shouldThrowIfOkEventHasNonIntegerValue() {
        long time = 0;
        Event event1 = createInvokeEvent(time++, PROCESS_0);
        Event event2 = createOkEvent(time++, PROCESS_0, "1");
        Event event3 = createInvokeEvent(time++, PROCESS_0);
        Event event4 = createOkEvent(time++, PROCESS_0, "noop");

        NonOverlappingReadsMonotonicChecker checker = new NonOverlappingReadsMonotonicChecker();
        assertThatThrownBy(() -> checker.check(ImmutableList.of(event1, event2, event3, event4)))
                .isInstanceOf(NumberFormatException.class);
    }

    @Test
    public void shouldParseLongValues() {
        long time = 0;
        Event event1 = createInvokeEvent(time++, PROCESS_0);
        Event event2 = createOkEvent(time++, PROCESS_0, INT_MAX_PLUS_ONE.toString());
        Event event3 = createInvokeEvent(time++, PROCESS_0);
        Event event4 = createOkEvent(time++, PROCESS_0, INT_MAX_PLUS_TWO.toString());

        CheckerTestUtils.assertNoErrors(NonOverlappingReadsMonotonicChecker::new,
                event1, event2, event3, event4);
    }

    private ImmutableInvokeEvent createInvokeEvent(long time, int process) {
        return ImmutableInvokeEvent.builder()
                .time(time)
                .process(process)
                .build();
    }

    private ImmutableOkEvent createOkEvent(long time, int process, String value) {
        return ImmutableOkEvent.builder()
                .time(time)
                .process(process)
                .value(value)
                .build();
    }

    private ImmutableFailEvent createFailEvent(long time, int process) {
        return ImmutableFailEvent.builder()
                .time(time)
                .process(process)
                .error("unknown")
                .build();
    }

    private static CheckerResult runChecker(Event... events) {
        com.palantir.atlasdb.jepsen.timestamp.NonOverlappingReadsMonotonicChecker checker =
                new com.palantir.atlasdb.jepsen.timestamp.NonOverlappingReadsMonotonicChecker();
        return checker.check(ImmutableList.copyOf(events));
    }
}

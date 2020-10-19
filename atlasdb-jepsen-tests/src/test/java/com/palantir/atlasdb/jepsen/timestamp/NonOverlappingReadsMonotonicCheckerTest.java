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
        Event event1 = TestEventUtils.invokeTimestamp(time++, PROCESS_0);
        Event event2 = TestEventUtils.timestampOk(time++, PROCESS_0, "1");
        Event event3 = TestEventUtils.invokeTimestamp(time++, PROCESS_0);
        Event event4 = TestEventUtils.timestampOk(time++, PROCESS_0, "0");

        CheckerResult result = runChecker(event1, event2, event3, event4);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(event2, event3, event4);
    }

    @Test
    public void shouldFailOnDecreasingConfirmedReadsAcrossTwoProcesses() {
        long time = 0;
        Event event1 = TestEventUtils.invokeTimestamp(time++, PROCESS_0);
        Event event2 = TestEventUtils.timestampOk(time++, PROCESS_0, "1");
        Event event3 = TestEventUtils.invokeTimestamp(time++, PROCESS_1);
        Event event4 = TestEventUtils.timestampOk(time++, PROCESS_1, "0");

        CheckerResult result = runChecker(event1, event2, event3, event4);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(event2, event3, event4);
    }

    @Test
    public void shouldFailOnEqualConfirmedReads() {
        long time = 0;
        Event event1 = TestEventUtils.invokeTimestamp(time++, PROCESS_0);
        Event event2 = TestEventUtils.timestampOk(time++, PROCESS_0, "0");
        Event event3 = TestEventUtils.invokeTimestamp(time++, PROCESS_0);
        Event event4 = TestEventUtils.timestampOk(time++, PROCESS_0, "0");

        CheckerResult result = runChecker(event1, event2, event3, event4);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(event2, event3, event4);
    }

    @Test
    public void shouldSucceedOnOverlappingReadsOnTwoProcesses() {
        long time = 0;
        Event event1 = TestEventUtils.invokeTimestamp(time++, PROCESS_0);
        Event event2 = TestEventUtils.invokeTimestamp(time++, PROCESS_1);
        Event event3 = TestEventUtils.timestampOk(time++, PROCESS_0, "1");
        Event event4 = TestEventUtils.timestampOk(time++, PROCESS_1, "0");

        CheckerTestUtils.assertNoErrors(NonOverlappingReadsMonotonicChecker::new,
                event1, event2, event3, event4);
    }

    @Test
    public void failureShouldNegateInvokeOnOneProcess() {
        long time = 0;
        Event event1 = TestEventUtils.invokeTimestamp(time++, PROCESS_0);
        Event event2 = TestEventUtils.createFailEvent(time++, PROCESS_0);
        Event event3 = TestEventUtils.invokeTimestamp(time++, PROCESS_0);
        Event event4 = TestEventUtils.timestampOk(time++, PROCESS_0, "1");

        CheckerTestUtils.assertNoErrors(NonOverlappingReadsMonotonicChecker::new,
                event1, event2, event3, event4);
    }

    @Test
    public void shouldIgnoreFailureOnOtherProcess() {
        long time = 0;
        Event event1 = TestEventUtils.invokeTimestamp(time++, PROCESS_0);
        Event event2 = TestEventUtils.createFailEvent(time++, PROCESS_0);
        Event event3 = TestEventUtils.invokeTimestamp(time++, PROCESS_1);
        Event event4 = TestEventUtils.timestampOk(time++, PROCESS_1, "1");

        CheckerTestUtils.assertNoErrors(NonOverlappingReadsMonotonicChecker::new,
                event1, event2, event3, event4);
    }

    @Test
    public void shouldIgnoreOverlappingFailure() {
        long time = 0;
        Event event1 = TestEventUtils.invokeTimestamp(time++, PROCESS_0);
        Event event3 = TestEventUtils.invokeTimestamp(time++, PROCESS_1);
        Event event2 = TestEventUtils.createFailEvent(time++, PROCESS_0);
        Event event4 = TestEventUtils.timestampOk(time++, PROCESS_1, "1");

        CheckerTestUtils.assertNoErrors(NonOverlappingReadsMonotonicChecker::new,
                event1, event2, event3, event4);
    }

    @Test
    public void shouldThrowIfOkEventHasNonIntegerValue() {
        long time = 0;
        Event event1 = TestEventUtils.invokeTimestamp(time++, PROCESS_0);
        Event event2 = TestEventUtils.timestampOk(time++, PROCESS_0, "1");
        Event event3 = TestEventUtils.invokeTimestamp(time++, PROCESS_0);
        Event event4 = TestEventUtils.timestampOk(time++, PROCESS_0, "noop");

        NonOverlappingReadsMonotonicChecker checker = new NonOverlappingReadsMonotonicChecker();
        assertThatThrownBy(() -> checker.check(ImmutableList.of(event1, event2, event3, event4)))
                .isInstanceOf(NumberFormatException.class);
    }

    @Test
    public void shouldParseLongValues() {
        long time = 0;
        Event event1 = TestEventUtils.invokeTimestamp(time++, PROCESS_0);
        Event event2 = TestEventUtils.timestampOk(time++, PROCESS_0, INT_MAX_PLUS_ONE.toString());
        Event event3 = TestEventUtils.invokeTimestamp(time++, PROCESS_0);
        Event event4 = TestEventUtils.timestampOk(time++, PROCESS_0, INT_MAX_PLUS_TWO.toString());

        CheckerTestUtils.assertNoErrors(NonOverlappingReadsMonotonicChecker::new,
                event1, event2, event3, event4);
    }


    private static CheckerResult runChecker(Event... events) {
        NonOverlappingReadsMonotonicChecker checker = new NonOverlappingReadsMonotonicChecker();
        return checker.check(ImmutableList.copyOf(events));
    }
}

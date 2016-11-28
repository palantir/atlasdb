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

import java.util.Arrays;

import org.junit.Test;

import com.palantir.atlasdb.jepsen.events.Event;
import com.palantir.atlasdb.jepsen.events.ImmutableFailEvent;
import com.palantir.atlasdb.jepsen.events.ImmutableInvokeEvent;
import com.palantir.atlasdb.jepsen.events.ImmutableOkEvent;

public class ImpossibleServerCheckerTest {
    private static final int PROCESS_0 = 0;
    private static final int PROCESS_1 = 1;

    @Test
    public void shouldPassOnNoEvents() {
        ImpossibleServerChecker checker = runChecker();

        assertThat(checker.valid()).isTrue();
        assertThat(checker.errors()).isEmpty();
    }

    @Test
    public void shouldFailOnDecreasingConfirmedReadsOnOneProcess() {
        long time = 0;
        Event event1 = createInvokeEvent(time++, PROCESS_0);
        Event event2 = createOkEvent(time++, PROCESS_0, 1L);
        Event event3 = createInvokeEvent(time++, PROCESS_0);
        Event event4 = createOkEvent(time++, PROCESS_0, 0L);

        ImpossibleServerChecker checker = runChecker(event1, event2, event3, event4);

        assertThat(checker.valid()).isFalse();
        assertThat(checker.errors()).containsExactly(event2, event3, event4);
    }

    @Test
    public void shouldFailOnDecreasingConfirmedReadsAcrossTwoProcesses() {
        long time = 0;
        Event event1 = createInvokeEvent(time++, PROCESS_0);
        Event event2 = createOkEvent(time++, PROCESS_0, 1L);
        Event event3 = createInvokeEvent(time++, PROCESS_1);
        Event event4 = createOkEvent(time++, PROCESS_1, 0L);

        ImpossibleServerChecker checker = runChecker(event1, event2, event3, event4);

        assertThat(checker.valid()).isFalse();
        assertThat(checker.errors()).containsExactly(event2, event3, event4);
    }

    @Test
    public void shouldFailOnEqualConfirmedReads() {
        long time = 0;
        Event event1 = createInvokeEvent(time++, PROCESS_0);
        Event event2 = createOkEvent(time++, PROCESS_0, 0L);
        Event event3 = createInvokeEvent(time++, PROCESS_0);
        Event event4 = createOkEvent(time++, PROCESS_0, 0L);

        ImpossibleServerChecker checker = runChecker(event1, event2, event3, event4);

        assertThat(checker.valid()).isFalse();
        assertThat(checker.errors()).containsExactly(event2, event3, event4);
    }

    @Test
    public void shouldFailOnOverlappingReadsOnTwoProcesses() {
        long time = 0;
        Event event1 = createInvokeEvent(time++, PROCESS_0);
        Event event2 = createInvokeEvent(time++, PROCESS_1);
        Event event3 = createOkEvent(time++, PROCESS_0, 1L);
        Event event4 = createOkEvent(time++, PROCESS_1, 0L);

        ImpossibleServerChecker checker = runChecker(event1, event2, event3, event4);

        assertThat(checker.valid()).isTrue();
        assertThat(checker.errors()).isEmpty();
    }

    @Test
    public void failureShouldNegateInvokeOnOneProcess() {
        long time = 0;
        Event event1 = createInvokeEvent(time++, PROCESS_0);
        Event event2 = createFailEvent(time++, PROCESS_0);
        Event event3 = createInvokeEvent(time++, PROCESS_0);
        Event event4 = createOkEvent(time++, PROCESS_0, 1L);

        ImpossibleServerChecker checker = runChecker(event1, event2, event3, event4);

        assertThat(checker.valid()).isTrue();
        assertThat(checker.errors()).isEmpty();
    }

    @Test
    public void shouldIgnoreFailureOnOtherProcess() {
        long time = 0;
        Event event1 = createInvokeEvent(time++, PROCESS_0);
        Event event2 = createFailEvent(time++, PROCESS_0);
        Event event3 = createInvokeEvent(time++, PROCESS_1);
        Event event4 = createOkEvent(time++, PROCESS_1, 1L);

        ImpossibleServerChecker checker = runChecker(event1, event2, event3, event4);

        assertThat(checker.valid()).isTrue();
        assertThat(checker.errors()).isEmpty();
    }

    @Test
    public void shouldIgnoreOverlappingFailure() {
        long time = 0;
        Event event1 = createInvokeEvent(time++, PROCESS_0);
        Event event3 = createInvokeEvent(time++, PROCESS_1);
        Event event2 = createFailEvent(time++, PROCESS_0);
        Event event4 = createOkEvent(time++, PROCESS_1, 1L);

        ImpossibleServerChecker checker = runChecker(event1, event2, event3, event4);

        assertThat(checker.valid()).isTrue();
        assertThat(checker.errors()).isEmpty();
    }

    private ImmutableInvokeEvent createInvokeEvent(long time, int process) {
        return ImmutableInvokeEvent.builder()
                .time(time)
                .process(process)
                .build();
    }

    private ImmutableOkEvent createOkEvent(long time, int process, long value) {
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

    private static ImpossibleServerChecker runChecker(Event... events) {
        ImpossibleServerChecker checker = new ImpossibleServerChecker();
        Arrays.asList(events).forEach(event -> event.accept(checker));
        return checker;
    }
}

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

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.jepsen.events.Event;
import com.palantir.atlasdb.jepsen.events.ImmutableOkEvent;

public class UniquenessCheckerTest {
    private static final long ZERO_TIME = 0L;
    private static final int PROCESS_0 = 0;
    private static final int PROCESS_1 = 1;
    private static final long VALUE_A = 0L;
    private static final long VALUE_B = 1L;

    @Test
    public void shouldSuceeedOnNoEvents() {
        CheckerResult result = runUniquenessChecker();

        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    @Test
    public void shouldSucceedOnUniqueValues() {
        Event event1 = ImmutableOkEvent.builder()
                .time(ZERO_TIME)
                .process(PROCESS_0)
                .value(VALUE_A)
                .build();
        Event event2 = ImmutableOkEvent.builder()
                .time(ZERO_TIME)
                .process(PROCESS_0)
                .value(VALUE_B)
                .build();

        CheckerResult result = runUniquenessChecker(event1, event2);

        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    @Test
    public void shouldRecordFourEventsIfThreeEqualValues() {
        long time = 0;
        Event event1 = ImmutableOkEvent.builder()
                .time(time++)
                .process(PROCESS_0)
                .value(VALUE_A)
                .build();
        Event event2 = ImmutableOkEvent.builder()
                .time(time++)
                .process(PROCESS_0)
                .value(VALUE_A)
                .build();
        Event event3 = ImmutableOkEvent.builder()
                .time(time++)
                .process(PROCESS_0)
                .value(VALUE_A)
                .build();

        CheckerResult result = runUniquenessChecker(event1, event2, event3);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(event1, event2, event2, event3);
    }

    @Test
    public void shouldFailIfSameValueAppearsOnTwoProcesses() {
        Event event1 = ImmutableOkEvent.builder()
                .time(ZERO_TIME)
                .process(PROCESS_0)
                .value(VALUE_A)
                .build();
        Event event2 = ImmutableOkEvent.builder()
                .time(ZERO_TIME)
                .process(PROCESS_1)
                .value(VALUE_A)
                .build();

        CheckerResult result = runUniquenessChecker(event1, event2);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(event1, event2);
    }

    @Test
    public void shouldFailIfSameValueAppearsAfterDifferentValue() {
        long time = 0;
        Event event1 = ImmutableOkEvent.builder()
                .time(time++)
                .process(PROCESS_0)
                .value(VALUE_A)
                .build();
        Event event2 = ImmutableOkEvent.builder()
                .time(time++)
                .process(PROCESS_1)
                .value(VALUE_B)
                .build();
        Event event3 = ImmutableOkEvent.builder()
                .time(time++)
                .process(PROCESS_1)
                .value(VALUE_A)
                .build();

        CheckerResult result = runUniquenessChecker(event1, event2, event3);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(event1, event3);
    }

    private static CheckerResult runUniquenessChecker(Event... events) {
        UniquenessChecker uniquenessChecker = new UniquenessChecker();
        return uniquenessChecker.check(ImmutableList.copyOf(events));
    }
}

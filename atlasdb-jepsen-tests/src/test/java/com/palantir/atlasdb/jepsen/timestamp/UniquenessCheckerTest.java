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

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.jepsen.CheckerResult;
import com.palantir.atlasdb.jepsen.events.Event;
import com.palantir.atlasdb.jepsen.utils.CheckerTestUtils;
import com.palantir.atlasdb.jepsen.utils.TestEventUtils;
import org.junit.Test;

public class UniquenessCheckerTest {
    private static final long ZERO_TIME = 0L;
    private static final int PROCESS_0 = 0;
    private static final int PROCESS_1 = 1;
    private static final String VALUE_A = "0";
    private static final String VALUE_B = "1";

    @Test
    public void shouldSuceeedOnNoEvents() {
        CheckerResult result = runUniquenessChecker();

        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }

    @Test
    public void shouldSucceedOnUniqueValues() {
        Event event1 = TestEventUtils.timestampOk(ZERO_TIME, PROCESS_0, VALUE_A);
        Event event2 = TestEventUtils.timestampOk(ZERO_TIME, PROCESS_0, VALUE_B);

        CheckerTestUtils.assertNoErrors(UniquenessChecker::new, event1, event2);
    }

    @Test
    public void shouldRecordFourEventsIfThreeEqualValues() {
        long time = 0;
        Event event1 = TestEventUtils.timestampOk(time++, PROCESS_0, VALUE_A);
        Event event2 = TestEventUtils.timestampOk(time++, PROCESS_0, VALUE_A);
        Event event3 = TestEventUtils.timestampOk(time++, PROCESS_0, VALUE_A);

        CheckerResult result = runUniquenessChecker(event1, event2, event3);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(event1, event2, event2, event3);
    }

    @Test
    public void shouldFailIfSameValueAppearsOnTwoProcesses() {
        Event event1 = TestEventUtils.timestampOk(ZERO_TIME, PROCESS_0, VALUE_A);
        Event event2 = TestEventUtils.timestampOk(ZERO_TIME, PROCESS_1, VALUE_A);

        CheckerResult result = runUniquenessChecker(event1, event2);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(event1, event2);
    }

    @Test
    public void shouldFailIfSameValueAppearsAfterDifferentValue() {
        long time = 0;
        Event event1 = TestEventUtils.timestampOk(time++, PROCESS_0, VALUE_A);
        Event event2 = TestEventUtils.timestampOk(time++, PROCESS_1, VALUE_B);
        Event event3 = TestEventUtils.timestampOk(time++, PROCESS_1, VALUE_A);

        CheckerResult result = runUniquenessChecker(event1, event2, event3);

        assertThat(result.valid()).isFalse();
        assertThat(result.errors()).containsExactly(event1, event3);
    }

    private static CheckerResult runUniquenessChecker(Event... events) {
        UniquenessChecker uniquenessChecker = new UniquenessChecker();
        return uniquenessChecker.check(ImmutableList.copyOf(events));
    }
}

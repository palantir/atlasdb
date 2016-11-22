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

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

public class MonotonicCheckerTest {
    public static final Long ZERO_TIME = 0L;
    public static final int PROCESS_0 = 0;
    public static final int PROCESS_1 = 1;

    @Test
    public void checkerShouldPassOnEmptyList() {
        MonotonicChecker monotonicChecker = runMonotonicChecker(new History());

        assertTrue(monotonicChecker.valid());
        assertThat(monotonicChecker.errors(), is(empty()));
    }

    @Test
    public void checkShouldFailOnDecreasingEntries() {
        History history = new History();

        Event event1 = new OkRead(ZERO_TIME, PROCESS_0, 1L);
        Event event2 = new OkRead(ZERO_TIME, PROCESS_0, 0L);

        history.add(event1);
        history.add(event2);

        MonotonicChecker monotonicChecker = runMonotonicChecker(history);

        assertFalse(monotonicChecker.valid());
        assertThat(monotonicChecker.errors(), equalTo(Arrays.asList(event1, event2)));
    }

    @Test
    public void checkShouldFailOnEqualEntries() {
        History history = new History();

        Event event1 = new OkRead(ZERO_TIME, PROCESS_0, 0L);
        Event event2 = new OkRead(ZERO_TIME, PROCESS_0, 0L);

        history.add(event1);
        history.add(event2);

        MonotonicChecker monotonicChecker = runMonotonicChecker(history);

        assertFalse(monotonicChecker.valid());
        assertThat(monotonicChecker.errors(), equalTo(Arrays.asList(event1, event2)));
    }

    @Test
    public void checkShouldPassOnTwoProcessesOverlapping() {
        History history = new History();

        Event event1 = new OkRead(ZERO_TIME, PROCESS_0, 1L);
        Event event2 = new OkRead(ZERO_TIME, PROCESS_1, 2L);
        Event event3 = new OkRead(ZERO_TIME, PROCESS_0, 4L);
        Event event4 = new OkRead(ZERO_TIME, PROCESS_1, 3L);

        history.add(event1);
        history.add(event2);
        history.add(event3);
        history.add(event4);

        MonotonicChecker monotonicChecker = runMonotonicChecker(history);

        assertTrue(monotonicChecker.valid());
        assertThat(monotonicChecker.errors(), is(empty()));
    }

    private MonotonicChecker runMonotonicChecker(History history) {
        MonotonicChecker monotonicChecker = new MonotonicChecker();
        history.accept(monotonicChecker);
        return monotonicChecker;
    }
}

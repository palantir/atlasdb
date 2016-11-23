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
import static org.assertj.core.api.Assertions.entry;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.jepsen.events.Event;
import com.palantir.atlasdb.jepsen.events.ImmutableOkEvent;

import clojure.lang.Keyword;

public class MonotonicCheckerTest {
    private static final Long ZERO_TIME = 0L;
    private static final int PROCESS_0 = 0;
    private static final int PROCESS_1 = 1;

    @Test
    public void shouldPassOnNoEvents() {
        Map<Keyword, Object> results = runMonotonicChecker();

        List<Event> expectedErrors = ImmutableList.of();
        assertThat(results).contains(entry(Keyword.intern("valid"), true));
        assertThat(results).contains(entry(Keyword.intern("errors"), expectedErrors));
    }

    @Test
    public void shouldFailOnDecreasingValueEvents() {
        Event event1 = ImmutableOkEvent.builder()
                .time(ZERO_TIME)
                .process(PROCESS_0)
                .value(1L)
                .build();
        Event event2 = ImmutableOkEvent.builder()
                .time(ZERO_TIME)
                .process(PROCESS_0)
                .value(0L)
                .build();

        Map<Keyword, Object> results = runMonotonicChecker(event1, event2);

        List<Event> expectedErrors = ImmutableList.of(event1, event2);
        assertThat(results).contains(entry(Keyword.intern("valid"), false));
        assertThat(results).contains(entry(Keyword.intern("errors"), expectedErrors));
    }

    @Test
    public void shouldFailOnEqualEntries() {
        Event event1 = ImmutableOkEvent.builder()
                .time(ZERO_TIME)
                .process(PROCESS_0)
                .value(0L)
                .build();
        Event event2 = ImmutableOkEvent.builder()
                .time(ZERO_TIME)
                .process(PROCESS_0)
                .value(0L)
                .build();

        Map<Keyword, Object> results = runMonotonicChecker(event1, event2);

        List<Event> expectedErrors = ImmutableList.of(event1, event2);
        assertThat(results).contains(entry(Keyword.intern("valid"), false),
                entry(Keyword.intern("errors"), expectedErrors));
    }

    @Test
    public void shouldPassOnTwoProcessesOverlapping() {
        Event event1 = ImmutableOkEvent.builder()
                .time(ZERO_TIME)
                .process(PROCESS_0)
                .value(1L)
                .build();
        Event event2 = ImmutableOkEvent.builder()
                .time(ZERO_TIME)
                .process(PROCESS_1)
                .value(2L)
                .build();
        Event event3 = ImmutableOkEvent.builder()
                .time(ZERO_TIME)
                .process(PROCESS_0)
                .value(4L)
                .build();
        Event event4 = ImmutableOkEvent.builder()
                .time(ZERO_TIME)
                .process(PROCESS_1)
                .value(3L)
                .build();

        Map<Keyword, Object> results = runMonotonicChecker(event1, event2, event3, event4);

        List<Event> expectedErrors = ImmutableList.of();
        assertThat(results).contains(entry(Keyword.intern("valid"), true));
        assertThat(results).contains(entry(Keyword.intern("errors"), expectedErrors));
    }

    private static Map<Keyword, Object> runMonotonicChecker(Event... events) {
        MonotonicChecker monotonicChecker = new MonotonicChecker();
        Arrays.asList(events).forEach(event -> event.accept(monotonicChecker));
        return monotonicChecker.results();
    }
}

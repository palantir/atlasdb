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
package com.palantir.atlasdb.jepsen;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import clojure.lang.Keyword;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.jepsen.events.Checker;
import com.palantir.atlasdb.jepsen.events.Event;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Test;

public class JepsenHistoryCheckerTest {
    private static final Map<Keyword, ?> INFO_EVENT = ImmutableMap.of(
            Keyword.intern("type"), "info",
            Keyword.intern("process"), JepsenConstants.NEMESIS_PROCESS,
            Keyword.intern("f"), "function",
            Keyword.intern("time"), 12345L);
    private static final Map<Keyword, ?> INVOKE_EVENT = ImmutableMap.of(
            Keyword.intern("type"), "invoke",
            Keyword.intern("process"), 0,
            Keyword.intern("f"), "function",
            Keyword.intern("time"), 0L);
    private static final Map<Keyword, ?> UNRECOGNISED_EVENT = ImmutableMap.of(Keyword.intern("foo"), "bar");

    @Test
    public void correctHistoryShouldReturnValidAndNoErrors() {
        Checker checker = createMockedChecker(true);

        Map<Keyword, Object> results = runJepsenChecker(checker);

        Map<Keyword, Object> expectedResults = ImmutableMap.of(Keyword.intern("valid?"), true,
                Keyword.intern("errors"), ImmutableList.of());
        assertThat(results).isEqualTo(expectedResults);
    }

    @Test
    public void incorrectHistoryShouldReturnInvalidWithErrors() {
        Checker checker = createMockedChecker(false, INFO_EVENT);

        Map<Keyword, Object> results = runJepsenChecker(checker);

        Map<Keyword, Object> expectedResults = ImmutableMap.of(Keyword.intern("valid?"), false,
                Keyword.intern("errors"), ImmutableList.of(INFO_EVENT));
        assertThat(results).isEqualTo(expectedResults);
    }

    @Test
    public void ifTwoCheckersFailErrorsAreCombined() {
        Checker firstChecker = createMockedChecker(false, INFO_EVENT);
        Checker secondChecker = createMockedChecker(false, INVOKE_EVENT);

        Map<Keyword, Object> results = runJepsenChecker(firstChecker, secondChecker);

        Map<Keyword, Object> expectedResults = ImmutableMap.of(Keyword.intern("valid?"), false,
                Keyword.intern("errors"), ImmutableList.of(INFO_EVENT, INVOKE_EVENT));
        assertThat(results).isEqualTo(expectedResults);
    }

    @Test
    public void invalidIfOneOutOfTwoCheckersFails() {
        Checker firstChecker = createMockedChecker(false, INFO_EVENT);
        Checker secondChecker = createMockedChecker(true);

        Map<Keyword, Object> results = runJepsenChecker(firstChecker, secondChecker);

        Map<Keyword, Object> expectedResults = ImmutableMap.of(Keyword.intern("valid?"), false,
                Keyword.intern("errors"), ImmutableList.of(INFO_EVENT));
        assertThat(results).isEqualTo(expectedResults);
    }

    @Test
    public void historyWithUnrecognisedShouldThrow() {
        Checker checker = mock(Checker.class);
        JepsenHistoryChecker jepsenChecker = new JepsenHistoryChecker(checker);
        assertThatThrownBy(() -> jepsenChecker.checkClojureHistory(ImmutableList.of(UNRECOGNISED_EVENT)))
                .isInstanceOf(Exception.class);
    }

    private Map<Keyword, Object> runJepsenChecker(Checker ... checkers) {
        JepsenHistoryChecker jepsenChecker = new JepsenHistoryChecker(checkers);
        return jepsenChecker.checkClojureHistory(ImmutableList.of(INFO_EVENT));
    }


    private Checker createMockedChecker(boolean valid, Map<Keyword, ?> ... errors) {
        List<Map<Keyword, ?>> listOfErrors = ImmutableList.copyOf(errors);
        List<Event> listOfErrorsAsEvents = listOfErrors.stream()
                .map(Event::fromKeywordMap)
                .collect(Collectors.toList());

        Checker checker = mock(Checker.class);
        CheckerResult result = ImmutableCheckerResult.builder()
                .valid(valid)
                .errors(listOfErrorsAsEvents)
                .build();
        when(checker.check(any())).thenReturn(result);
        return checker;
    }
}

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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.jepsen.events.Event;
import com.palantir.atlasdb.jepsen.events.EventVisitor;

import clojure.lang.Keyword;

public class TimestampCheckerUnitTest {
    private static final Map<Keyword, ?> INFO_EVENT = ImmutableMap.of(Keyword.intern("type"), Keyword.intern("info"));
    private static final Map<Keyword, ?> UNRECOGNISED_EVENT = ImmutableMap.of(Keyword.intern("foo"), "bar");

    @Test
    public void correctHistoryShouldReturnValidAndNoErrors() {
        EventVisitor checker = mock(EventVisitor.class);
        when(checker.valid()).thenReturn(true);
        when(checker.errors()).thenReturn(ImmutableList.of());

        Map<Keyword, Object> results = new TimestampChecker(checker).checkClojureHistory(ImmutableList.of(INFO_EVENT));

        Map<Keyword, Object> expectedResults = ImmutableMap.of(Keyword.intern("valid?"), true,
                Keyword.intern("errors"), ImmutableList.of());
        assertThat(results).isEqualTo(expectedResults);
    }

    @Test
    public void incorrectHistoryShouldReturnInvalidWithErrors() {
        EventVisitor checker = mock(EventVisitor.class);
        when(checker.valid()).thenReturn(false);
        when(checker.errors()).thenReturn(ImmutableList.of(Event.fromKeywordMap(INFO_EVENT)));

        Map<Keyword, Object> results = new TimestampChecker(checker).checkClojureHistory(ImmutableList.of(INFO_EVENT));

        Map<Keyword, Object> expectedResults = ImmutableMap.of(Keyword.intern("valid?"), false,
                Keyword.intern("errors"), ImmutableList.of(INFO_EVENT));
        assertThat(results).isEqualTo(expectedResults);
    }

    @Test
    public void historyWithUnrecognisedShouldThrow() {
        EventVisitor checker = mock(EventVisitor.class);

        assertThatThrownBy(() -> new TimestampChecker(checker).checkClojureHistory(
                ImmutableList.of(UNRECOGNISED_EVENT))).isInstanceOf(Exception.class);
    }
}

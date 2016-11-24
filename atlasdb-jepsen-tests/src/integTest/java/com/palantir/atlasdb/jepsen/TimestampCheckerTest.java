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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;

import clojure.lang.Keyword;
import one.util.streamex.EntryStream;

public class TimestampCheckerTest {
    public static final int SOME_PROCESS = 0;
    public static final long TIME_0 = 0L;
    public static final long TIME_1 = 1L;

    @Test
    public void correctExampleHistoryShouldReturnValidAndNoErrors() throws IOException {
        List<Map<Keyword, ?>> convertedAllEvents = getClojureMapFromFile("history.json");

        Map<Keyword, Object> results = TimestampChecker.checkClojureHistory(convertedAllEvents);

        assertThat(results).containsEntry(Keyword.intern("valid?"), true);
        assertThat(results).containsEntry(Keyword.intern("errors"), ImmutableList.of());
    }

    @Test
    public void correctHistoryShouldReturnValidAndNoErrors() {
        Map<Keyword, ?> invokeRead = ImmutableMap.of(Keyword.intern("type"), "invoke",
                Keyword.intern("process"), SOME_PROCESS,
                Keyword.intern("time"), TIME_0);
        Map<Keyword, ?> okRead = ImmutableMap.of(Keyword.intern("type"), "ok",
                Keyword.intern("process"), SOME_PROCESS,
                Keyword.intern("time"), TIME_1,
                Keyword.intern("value"), 0L);
        List<Map<Keyword, ?>> history = ImmutableList.of(invokeRead, okRead);

        Map<Keyword, Object> results = TimestampChecker.checkClojureHistory(history);

        assertThat(results).containsEntry(Keyword.intern("valid?"), true);
        assertThat(results).containsEntry(Keyword.intern("errors"), ImmutableList.of());
    }

    @Test
    public void historyOfDecreasingTimestampsShouldReturnInvalidWithErrors() {
        Map<Keyword, ?> read1 = ImmutableMap.of(Keyword.intern("type"), Keyword.intern("ok"),
                Keyword.intern("process"), SOME_PROCESS,
                Keyword.intern("time"), TIME_0,
                Keyword.intern("value"), 1L);
        Map<Keyword, ?> read2 = ImmutableMap.of(Keyword.intern("type"), Keyword.intern("ok"),
                Keyword.intern("process"), SOME_PROCESS,
                Keyword.intern("time"), TIME_1,
                Keyword.intern("value"), 0L);
        List<Map<Keyword, ?>> history = ImmutableList.of(read1, read2);

        Map<Keyword, Object> results = TimestampChecker.checkClojureHistory(history);

        List<Map<Keyword, ?>> expectedErrors = ImmutableList.of(read1, read2);
        assertThat(results).containsEntry(Keyword.intern("valid?"), false);
        assertThat(results).containsEntry(Keyword.intern("errors"), expectedErrors);
    }

    @Test
    public void historyWithUnrecognisedEventsShouldThrow() {
        Map<Keyword, ?> unparsableEvent = ImmutableMap.of(Keyword.intern("foo"), "bar");
        List<Map<Keyword, ?>> history = ImmutableList.of(unparsableEvent);

        assertThatThrownBy(() -> TimestampChecker.checkClojureHistory(history)).isInstanceOf(Exception.class);
    }

    private static List<Map<Keyword, ?>> getClojureMapFromFile(String resourcePath) throws IOException {
        List<Map<String, ?>> allEvents = new ObjectMapper().readValue(Resources.getResource(resourcePath),
                new TypeReference<List<Map<String, ?>>>() {});
        return allEvents.stream()
                .map(singleEvent -> {
                    Map<Keyword, Object> convertedEvent = new HashMap<>();
                    EntryStream.of(singleEvent)
                            .mapKeys(Keyword::intern)
                            .mapValues(value -> value instanceof String ? Keyword.intern((String) value) : value)
                            .forKeyValue(convertedEvent::put);
                    return convertedEvent;
                })
                .collect(Collectors.toList());
    }
}

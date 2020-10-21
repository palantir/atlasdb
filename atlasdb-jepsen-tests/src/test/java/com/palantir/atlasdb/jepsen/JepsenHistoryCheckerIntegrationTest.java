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

import clojure.lang.Keyword;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.palantir.atlasdb.jepsen.events.Checker;
import com.palantir.common.streams.KeyedStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.junit.Test;

public class JepsenHistoryCheckerIntegrationTest {
    @Test
    public void correctExampleHistoryShouldReturnValidAndNoErrors() throws IOException {
        List<Map<Keyword, ?>> convertedAllEvents = getClojureMapFromFile("correct_history.json");

        Map<Keyword, Object> results =
                JepsenHistoryCheckers.createWithTimestampCheckers().checkClojureHistory(convertedAllEvents);

        assertThat(results).containsEntry(Keyword.intern("valid?"), true);
        assertThat(results).containsEntry(Keyword.intern("errors"), ImmutableList.of());
    }

    @Test
    public void correctLockTestHistoryShouldReturnValidAndNoErrors() throws IOException {
        List<Map<Keyword, ?>> convertedAllEvents = getClojureMapFromFile("lock_test_without_nemesis.json");

        Map<Keyword, Object> results =
                JepsenHistoryCheckers.createWithLockCheckers().checkClojureHistory(convertedAllEvents);

        assertThat(results).containsEntry(Keyword.intern("valid?"), true);
        assertThat(results).containsEntry(Keyword.intern("errors"), ImmutableList.of());
    }

    @Test
    public void livenessFailingHistoryShouldReturnInvalidWithNemesisErrors() throws IOException {
        List<Map<Keyword, ?>> convertedAllEvents = getClojureMapFromFile("liveness_failing_history.json");

        Map<Keyword, Object> results = JepsenHistoryCheckers.createWithCheckers(
                        ImmutableList.<Supplier<Checker>>builder()
                                .addAll(JepsenHistoryCheckers.TIMESTAMP_CHECKERS)
                                .add(NemesisResilienceChecker::new)
                                .build())
                .checkClojureHistory(convertedAllEvents);

        Map<Keyword, ?> nemesisStartEventMap = ImmutableMap.of(
                Keyword.intern("f"), "start",
                Keyword.intern("process"), JepsenConstants.NEMESIS_PROCESS,
                Keyword.intern("type"), "info",
                Keyword.intern("value"), "start!",
                Keyword.intern("time"), 18784227842L);
        Map<Keyword, ?> nemesisStopEventMap = ImmutableMap.of(
                Keyword.intern("f"), "stop",
                Keyword.intern("process"), JepsenConstants.NEMESIS_PROCESS,
                Keyword.intern("type"), "info",
                Keyword.intern("value"), "stop!",
                Keyword.intern("time"), 18805796986L);
        List<Map<Keyword, ?>> expected = ImmutableList.of(nemesisStartEventMap, nemesisStopEventMap);

        assertThat(results).containsEntry(Keyword.intern("valid?"), false);
        assertThat(results).containsEntry(Keyword.intern("errors"), expected);
    }

    private static List<Map<Keyword, ?>> getClojureMapFromFile(String resourcePath) throws IOException {
        List<Map<String, ?>> allEvents = new ObjectMapper()
                .readValue(Resources.getResource(resourcePath), new TypeReference<List<Map<String, ?>>>() {});
        return allEvents.stream()
                .map(singleEvent -> KeyedStream.stream(singleEvent)
                        .mapKeys(key -> Keyword.intern(key))
                        .map(value -> value instanceof String ? Keyword.intern((String) value) : value)
                        .collectToMap())
                .collect(Collectors.toList());
    }
}

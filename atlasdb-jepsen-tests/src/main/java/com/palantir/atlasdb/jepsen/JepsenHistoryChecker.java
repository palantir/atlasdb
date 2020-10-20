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

import clojure.lang.Keyword;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.jepsen.events.Checker;
import com.palantir.atlasdb.jepsen.events.Event;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JepsenHistoryChecker {

    private List<Checker> checkers;

    public JepsenHistoryChecker(Checker... checkers) {
        this.checkers = ImmutableList.copyOf(checkers);
    }

    public JepsenHistoryChecker(List<Checker> checkers) {
        this.checkers = ImmutableList.copyOf(checkers);
    }

    public List<Checker> getCheckers() {
        return checkers;
    }

    /**
     * Parses a history of events from a Jepsen test of the timestamp service, and verifies that it fits the model.
     * In particular, the timestamp values should be monotonically increasing for each process. See MonotonicChecker for
     * more details.
     *
     * @param clojureHistory A history of events. This is a list of maps, for example:
     *     [{":type": "invoke", "process": 0, "time", 0L},
     *      {":type": "ok",     "process": 0, "time": 0L, "value", 10L}]
     * @return A map of
     *     :valid?     A boolean of whether the check passes
     *     :errors     A list of events that failed the check, or an empty list if the check passed
     * @throws RuntimeException if the parsing of the history fails.
     */
    public Map<Keyword, Object> checkClojureHistory(List<Map<Keyword, ?>> clojureHistory) {
        List<Event> events = convertClojureHistoryToEventList(clojureHistory);
        return checkHistory(events);
    }

    private static List<Event> convertClojureHistoryToEventList(List<Map<Keyword, ?>> clojureHistory) {
        return clojureHistory.stream().map(Event::fromKeywordMap).collect(Collectors.toList());
    }

    private Map<Keyword, Object> checkHistory(List<Event> events) {
        List<CheckerResult> allResults =
                checkers.stream().map(checker -> checker.check(events)).collect(Collectors.toList());
        return createClojureMapFromResults(CheckerResult.combine(allResults));
    }

    private static Map<Keyword, Object> createClojureMapFromResults(CheckerResult results) {
        List<Map<Keyword, Object>> errorsAsClojureHistory = convertEventListToClojureHistory(results.errors());
        return ImmutableMap.of(
                Keyword.intern("valid?"), results.valid(), Keyword.intern("errors"), errorsAsClojureHistory);
    }

    private static List<Map<Keyword, Object>> convertEventListToClojureHistory(List<Event> events) {
        return events.stream().map(Event::toKeywordMap).collect(Collectors.toList());
    }
}

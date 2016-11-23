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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.palantir.atlasdb.jepsen.events.Event;

import clojure.lang.Keyword;

public final class TimestampChecker {
    private TimestampChecker() {
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
     *     :valid      A boolean of whether the check passes
     *     :errors     A list of events that failed the check, or an empty list if the check passed
     * @throws Exception if the parsing of the history fails.
     */
    public static Map<Keyword, Object> checkClojureHistory(List<Map<Keyword, ?>> clojureHistory) {
        List<Event> events = convertClojureHistoryToEventList(clojureHistory);
        return checkHistory(events);

    }

    private static List<Event> convertClojureHistoryToEventList(List<Map<Keyword, ?>> clojureHistory) {
        return clojureHistory.stream()
                .map(Event::fromKeywordMap)
                .collect(Collectors.toList());
    }

    private static Map<Keyword, Object> checkHistory(List<Event> events) {
        MonotonicChecker monotonicChecker = new MonotonicChecker();
        events.forEach(event -> event.accept(monotonicChecker));
        return monotonicChecker.results();
    }

}



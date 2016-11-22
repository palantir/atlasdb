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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.jepsen.events.Event;

import clojure.lang.Keyword;

public final class TimestampChecker {
    private static final Logger log = LoggerFactory.getLogger(TimestampChecker.class);

    private TimestampChecker() {
    }

    public static boolean checkClojureHistory(List<Map<Keyword, ?>> clojureHistory) {
        List<Event> events = convertClojureHistoryToEventList(clojureHistory);
        return events != null && checkHistory(events);

    }

    private static List<Event> convertClojureHistoryToEventList(List<Map<Keyword, ?>> clojureHistory) {
        try {
            return clojureHistory.stream()
                    .map(Event::fromKeywordMap)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("Failed to parse output from Jepsen: ", e);
            return null;
        }
    }

    private static boolean checkHistory(List<Event> events) {
        try {
            MonotonicChecker monotonicChecker = new MonotonicChecker();
            events.forEach(event -> event.accept(monotonicChecker));

            if (!monotonicChecker.valid()) {
                log.error("Check of monotonicity failed: {}", monotonicChecker.errors());
                return false;
            }
        } catch (Exception e) {
            log.error("An error occurred while checking the history", e);
            return false;
        }

        log.debug("Checking the history was successful");
        return true;
    }

}



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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentVector;

public final class TimestampChecker {
    private static final Logger log = LoggerFactory.getLogger(TimestampChecker.class);

    private TimestampChecker() {
    }

    public static boolean checkClojureHistory(PersistentVector clojureHistory) {
        History history = parse(clojureHistory);
        if (history == null) {
            return false;
        }

        return checkHistory(history);
    }

    private static History parse(PersistentVector clojureHistory) {
        try {
            List<ParsedMap> parsed = parseIntoJavaObjects(clojureHistory);
            return parseIntoEvents(parsed);
        } catch (Exception e) {
            log.error("Failed to parse output from Jepsen : " + e);
            return null;
        }
    }

    private static boolean checkHistory(History history) {
        try {
            MonotonicChecker monotonicChecker = new MonotonicChecker();
            history.accept(monotonicChecker);
            if (!monotonicChecker.valid()) {
                log.error("Check of monotonicity fails:");
                log.error(Arrays.toString(monotonicChecker.errors().toArray()));
                return false;
            }
        } catch (Exception e) {
            log.error("Errored during check: " + e);
            return false;
        }

        log.info("Everything OK");
        return true;
    }

    private static History parseIntoEvents(List<ParsedMap> parsedHistory) {
        History history = new History();
        for (ParsedMap m : parsedHistory) {
            history.parseAndAdd(m);
        }
        return history;
    }

    private static List<ParsedMap> parseIntoJavaObjects(PersistentVector history) {
        List<ParsedMap> parsedHistory = new ArrayList();
        for (Object e : history) {
            PersistentArrayMap map = (PersistentArrayMap) e;
            ParsedMap parsedMap = ParsedMap.create(map);
            parsedHistory.add(parsedMap);
        }
        return parsedHistory;
    }
}



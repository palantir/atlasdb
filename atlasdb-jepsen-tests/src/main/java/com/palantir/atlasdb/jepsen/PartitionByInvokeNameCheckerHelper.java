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

import com.palantir.atlasdb.jepsen.events.Checker;
import com.palantir.atlasdb.jepsen.events.Event;
import com.palantir.atlasdb.jepsen.events.InfoEvent;
import com.palantir.atlasdb.jepsen.events.InvokeEvent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class PartitionByInvokeNameCheckerHelper implements Checker {
    private final Supplier<Checker> checkerSupplier;

    public PartitionByInvokeNameCheckerHelper(Supplier<Checker> checkerSupplier) {
        this.checkerSupplier = checkerSupplier;
    }

    @Override
    public CheckerResult check(List<Event> events) {
        Map<String, List<Event>> histories = partitionEventsByInvokeValue(events);
        List<CheckerResult> results = runCheckerOnEachHistory(histories.values());
        return combineResults(results);
    }

    private Map<String, List<Event>> partitionEventsByInvokeValue(List<Event> events) {
        Map<String, List<Event>> partitionedEvents = new HashMap<>();
        Map<Integer, String> lastInvokeValueForProcess = new HashMap<>();
        for (Event event : events) {
            int process = event.process();
            if (event instanceof InvokeEvent) {
                InvokeEvent invokeEvent = (InvokeEvent) event;
                lastInvokeValueForProcess.put(process, invokeEvent.value());
            }
            String key = lastInvokeValueForProcess.getOrDefault(process, null);
            /*
             * Note that all InfoEvents get mapped to null
             */
            if (event instanceof InfoEvent) {
                key = null;
            }
            List<Event> history = partitionedEvents.getOrDefault(key, new ArrayList<>());
            history.add(event);
            partitionedEvents.put(key, history);
        }
        return partitionedEvents;
    }

    private List<CheckerResult> runCheckerOnEachHistory(Collection<List<Event>> histories) {
        return histories.stream().map(this::runChecker).collect(Collectors.toList());
    }

    private CheckerResult runChecker(List<Event> history) {
        Checker checker = checkerSupplier.get();
        return checker.check(history);
    }

    private CheckerResult combineResults(List<CheckerResult> results) {
        List<Event> allErrors = results.stream().flatMap(result ->
                result.errors().stream()).collect(Collectors.toList());
        boolean allValid = results.stream().allMatch(CheckerResult::valid);
        return ImmutableCheckerResult.builder()
                .valid(allValid)
                .errors(allErrors)
                .build();
    }
}

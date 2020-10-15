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
package com.palantir.atlasdb.jepsen.timestamp;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.jepsen.CheckerResult;
import com.palantir.atlasdb.jepsen.ImmutableCheckerResult;
import com.palantir.atlasdb.jepsen.events.Checker;
import com.palantir.atlasdb.jepsen.events.Event;
import com.palantir.atlasdb.jepsen.events.EventVisitor;
import com.palantir.atlasdb.jepsen.events.FailEvent;
import com.palantir.atlasdb.jepsen.events.OkEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UniquenessChecker implements Checker {
    @Override
    public CheckerResult check(List<Event> events) {
        Visitor visitor = new Visitor();
        events.forEach(event -> event.accept(visitor));
        return ImmutableCheckerResult.builder()
                .valid(visitor.valid())
                .errors(visitor.errors())
                .build();
    }

    private static class Visitor implements EventVisitor {
        private final List<Event> errors = new ArrayList<>();
        private final Map<String, OkEvent> valuesAlreadySeen = new HashMap<>();

        @Override
        public void visit(OkEvent event) {
            String value = event.value();

            if (valuesAlreadySeen.containsKey(value)) {
                OkEvent previousEvent = valuesAlreadySeen.get(value);
                errors.add(previousEvent);
                errors.add(event);
            }

            valuesAlreadySeen.put(value, event);
        }

        @Override
        public void visit(FailEvent event) {
        }

        public boolean valid() {
            return errors.isEmpty();
        }

        public List<Event> errors() {
            return ImmutableList.copyOf(errors);
        }
    }
}

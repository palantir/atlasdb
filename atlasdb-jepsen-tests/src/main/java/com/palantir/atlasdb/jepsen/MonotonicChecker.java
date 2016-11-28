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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.jepsen.events.Checker;
import com.palantir.atlasdb.jepsen.events.Event;
import com.palantir.atlasdb.jepsen.events.InfoEvent;
import com.palantir.atlasdb.jepsen.events.InvokeEvent;
import com.palantir.atlasdb.jepsen.events.OkEvent;

public class MonotonicChecker implements Checker {
    private final List<Event> errors = new ArrayList<>();
    private final Map<Integer, OkEvent> latestEventPerProcess = new HashMap<>();

    private boolean valid = true;

    @Override
    public void visit(InfoEvent event) {
    }

    @Override
    public void visit(InvokeEvent event) {
    }

    @Override
    public void visit(OkEvent event) {
        int process = event.process();

        if (latestEventPerProcess.containsKey(process)) {
            OkEvent previousEvent = latestEventPerProcess.get(process);
            if (event.value() <= previousEvent.value()) {
                valid = false;
                errors.add(previousEvent);
                errors.add(event);
            }
        }
        latestEventPerProcess.put(process, event);
    }

    @Override
    public boolean valid() {
        return valid;
    }

    @Override
    public List<Event> errors() {
        return ImmutableList.copyOf(errors);
    }
}

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

public class MonotonicChecker implements Visitor {
    boolean valid = true;
    List<Event> errors = new ArrayList();
    Map<Integer, Last> lastPerProcess = new HashMap<>();

    private static class Last {
        Long lastSeen = null;
        Event lastEvent = null;
    }

    public void visit(OkRead event) {
        Integer process = event.process();
        lastPerProcess.putIfAbsent(process, new Last());
        Last last = lastPerProcess.get(process);
        Long value = event.value();
        if (last.lastSeen != null && value <= last.lastSeen) {
            valid = false;
            errors.add(last.lastEvent);
            errors.add(event);
        }
        last.lastSeen = value;
        last.lastEvent = event;
    }

    public void visit(InvokeRead event) {
    }

    public boolean valid() {
        return valid;
    }

    public List<Event> errors() {
        return errors;
    }
}

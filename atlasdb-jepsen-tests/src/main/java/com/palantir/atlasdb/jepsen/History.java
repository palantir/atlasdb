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
import java.util.List;

public class History implements Visitable {
    List<Event> events = new ArrayList();

    public void add(Event event) {
        events.add(event);
    }

    public void accept(Visitor visitor) {
        for (Event event : events) {
            event.accept(visitor);
        }
    }

    public void parseAndAdd(ParsedMap map) {
        if (!map.type().isPresent()) {
            throw new IllegalArgumentException("Cannot process entries without types");
        }
        ParsedMap.Type type = map.type().get();
        switch (type) {
            case INVOKE: {
                String process = map.process().get();
                add(new InvokeRead(map.time().get(), Integer.parseInt(process)));
                break;
            }
            case OK: {
                String process = map.process().get();
                String value = map.value().get();
                add(new OkRead(map.time().get(), Integer.parseInt(process), Long.parseLong(value)));
                break;
            }
            case INFO:
                break;
            default:
                throw new IllegalArgumentException("Unhandled type " + type.name());
        }
    }
}

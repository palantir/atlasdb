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
package com.palantir.atlasdb.jepsen.utils;

import clojure.lang.Keyword;
import com.palantir.atlasdb.jepsen.JepsenConstants;
import com.palantir.atlasdb.jepsen.events.OkEvent;
import com.palantir.atlasdb.jepsen.events.RequestType;
import java.util.HashMap;
import java.util.Map;

public final class EventUtils {
    private EventUtils() {
        // utility class
    }

    public static boolean isFailure(OkEvent event) {
        switch (event.function()) {
            case RequestType.UNLOCK:
                return event.value().equals(OkEvent.UNLOCK_FAILURE);
            case RequestType.LOCK:
                return event.value().equals(OkEvent.LOCK_FAILURE);
            case RequestType.REFRESH:
                return event.value().equals(OkEvent.REFRESH_FAILURE);
            case RequestType.TIMESTAMP:
                return event.value().equals(OkEvent.TIMESTAMP_FAILURE);
            default:
                return false;
        }
    }

    public static Map<Keyword, ?> encodeNemesis(Map<Keyword, ?> clojureEvent) {
        Map<Keyword, Object> convertedEvent = new HashMap<>();
        for (Map.Entry<Keyword, ?> entry : clojureEvent.entrySet()) {
            if (entry.getKey().equals(Keyword.intern("process"))
                    && entry.getValue().equals(Keyword.intern(JepsenConstants.NEMESIS_PROCESS_NAME))) {
                convertedEvent.put(entry.getKey(), -1);
            } else {
                convertedEvent.put(entry.getKey(), entry.getValue());
            }
        }
        return convertedEvent;
    }
}

/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.jepsen.utils;

import com.palantir.atlasdb.jepsen.events.OkEvent;
import com.palantir.atlasdb.jepsen.events.RequestType;

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

    public static boolean isSuccessful(OkEvent event) {
        return !isFailure(event);
    }
}

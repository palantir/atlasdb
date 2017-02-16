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
package com.palantir.timestamp;

import java.util.Optional;
import java.util.UUID;

public class TimestampBoundStoreEntry {
    private static final Long INITIAL_VALUE = 10000L;
    public Optional<UUID> id;
    public Optional<Long> timestamp;

    protected TimestampBoundStoreEntry(Long timestamp, UUID id) {
        this.timestamp = Optional.ofNullable(timestamp);
        this.id = Optional.ofNullable(id);
    }

    public static TimestampBoundStoreEntry create(Long timestamp, UUID id) {
        return new TimestampBoundStoreEntry(timestamp, id);
    }

    public long getTimestampOrInitialValue() {
        return timestamp.orElse(INITIAL_VALUE);
    }

    public boolean idMatches(UUID otherId) {
        return id.map(otherId::equals).orElse(false);
    }

    public String getTimestampAsString() {
        return timestamp.map(ts -> Long.toString(ts)).orElse("none");
    }

    public String getIdAsString() {
        return id.map(UUID::toString).orElse("none");
    }
}

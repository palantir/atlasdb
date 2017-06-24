/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.lock.v2;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.palantir.lock.LockRefreshToken;

// TODO: also return a start timestamp here?
public class LockImmutableTimestampResponse {

    private final long immutableTimestamp;
    private final LockRefreshToken lock;

    public LockImmutableTimestampResponse(
            @JsonProperty("immutableTimestamp") long immutableTimestamp,
            @JsonProperty("lock") LockRefreshToken lock) {
        this.immutableTimestamp = immutableTimestamp;
        this.lock = lock;
    }

    @JsonProperty("immutableTimestamp")
    public long getImmutableTimestamp() {
        return immutableTimestamp;
    }

    @JsonProperty("lock")
    public LockRefreshToken getLock() {
        return lock;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LockImmutableTimestampResponse that = (LockImmutableTimestampResponse) o;
        return immutableTimestamp == that.immutableTimestamp &&
                Objects.equals(lock, that.lock);
    }

    @Override
    public int hashCode() {
        return Objects.hash(immutableTimestamp, lock);
    }
}

/*
 * Copyright 2018 Palantir Technologies
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

package com.palantir.atlasdb.compact;

import java.util.concurrent.TimeUnit;

import org.immutables.value.Value;

@Value.Immutable
public interface CompactorConfig {
    long DEFAULT_COMPACT_CONNECTION_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(10);
    long DEFAULT_COMPACT_PAUSE_ON_FAILURE_MILLIS = TimeUnit.SECONDS.toMillis(1800);
    long DEFAULT_COMPACT_PAUSE_MILLIS = TimeUnit.SECONDS.toMillis(10);

    /**
     * Indicates whether 
     * @return
     */
    @Value.Default
    default boolean enableCompaction() {
        return false;
    }

    @Value.Default
    default boolean inMaintenanceHours() {
        return false;
    }

    @Value.Default
    default long compactConnectionTimeoutMillis() {
        return DEFAULT_COMPACT_CONNECTION_TIMEOUT_MILLIS;
    }

    @Value.Default
    default long compactPauseOnFailureMillis() {
        return DEFAULT_COMPACT_PAUSE_ON_FAILURE_MILLIS;
    }

    @Value.Default
    default long compactPauseMillis() {
        return DEFAULT_COMPACT_PAUSE_MILLIS;
    }
}

/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

import com.google.common.base.Preconditions;

@Value.Immutable
public interface CompactorConfig {
    long DEFAULT_COMPACT_PAUSE_ON_FAILURE_MILLIS = TimeUnit.SECONDS.toMillis(1800);
    long DEFAULT_COMPACT_PAUSE_MILLIS = TimeUnit.SECONDS.toMillis(10);

    /**
     * Indicates whether background compaction should run at all.
     */
    @Value.Default
    default boolean enableCompaction() {
        return false;
    }

    /**
     * Indicates whether the background compactor is configured to run in maintenance mode.
     *
     * Compactors that are running in maintenance mode may perform more aggressive operations that could have
     * significant impacts on other queries to the underlying key-value service (such as blocking, un-cancelable
     * calls that acquire a table lock).
     */
    @Value.Default
    default boolean inMaintenanceMode() {
        return false;
    }

    /**
     * Indicates the time interval to wait after a failed compaction before trying again.
     */
    @Value.Default
    default long compactPauseOnFailureMillis() {
        return DEFAULT_COMPACT_PAUSE_ON_FAILURE_MILLIS;
    }

    /**
     * Indicates the time interval to wait after a successful compaction before trying again (e.g. on a new table).
     */
    @Value.Default
    default long compactPauseMillis() {
        return DEFAULT_COMPACT_PAUSE_MILLIS;
    }

    @Value.Check
    default void checkIntervalsNonnegative() {
        Preconditions.checkState(compactPauseOnFailureMillis() >= 0,
                "Compact pause-on-failure interval must be nonnegative, but found %s", compactPauseOnFailureMillis());
        Preconditions.checkState(compactPauseMillis() >= 0,
                "Compact pause interval must be nonnegative, but found %s", compactPauseMillis());
    }
}

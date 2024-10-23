/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.queue;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.time.Duration;
import java.util.function.Supplier;

public interface NumberOfShardsProvider {
    int getNumberOfShards();

    default int getNumberOfShards(SweeperStrategy strategy) {
        switch (strategy) {
            case THOROUGH:
            case CONSERVATIVE:
                return getNumberOfShards();
            case NON_SWEEPABLE:
                return AtlasDbConstants.TARGETED_SWEEP_NONE_SHARDS;
            default:
                throw new SafeIllegalArgumentException("Unknown sweep strategy", SafeArg.of("strategy", strategy));
        }
    }

    private static int getNumberOfShards(
            ShardProgress progress, Supplier<Integer> shards, MismatchBehaviour mismatchBehaviour) {
        switch (mismatchBehaviour) {
            case IGNORE_UPDATES:
                // Here, we will never update the number of shards from config. This is because the ASTS work requires
                // knowing the number of shards at all time from the beginning of install, and changing is quite painful
                return progress.getNumberOfShards();
            case UPDATE:
                return progress.updateNumberOfShards(shards.get());
            default:
                throw new SafeIllegalStateException(
                        "Unknown mismatch behaviour", SafeArg.of("mismatchBehaviour", mismatchBehaviour));
        }
    }

    /**
     * Creates a supplier such that the first call to {@link Supplier#get()} on it will take the maximum of the runtime
     * configuration and the persisted number of shards, and persist and memoize the result. Subsequent calls will
     * return the cached value until refreshTimeMillis has passed, at which point the next call will again perform the
     * check and set.
     *
     * @param runtimeShards     live reloadable runtime configuration for the number of shards
     * @param progress          progress table persisting the number of shards
     * @param mismatchBehaviour behaviour when the runtime configuration and persisted number of shards do not match
     * @param refreshTime       timeout for caching the number of shards
     * @return supplier calculating and persisting the number of shards to use
     */
    static NumberOfShardsProvider createMemoizingProvider(
            ShardProgress progress,
            Supplier<Integer> runtimeShards,
            MismatchBehaviour mismatchBehaviour,
            Duration refreshTime) {
        return Suppliers.memoizeWithExpiration(
                () -> getNumberOfShards(progress, runtimeShards, mismatchBehaviour), refreshTime)::get;
    }

    enum MismatchBehaviour {
        IGNORE_UPDATES,
        UPDATE,
    }
}

/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.table.description;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Optional;

/**
 * Represents the properties that the user has picked wrt sweep. The sweeper strategy is the mode of sweep that the
 * sweeper should apply on the table. mustCheckImmutableLockAfterReads indicates if a transaction must check the
 * immutable lock after migrating to this mode.
 */
public final class SweepStrategy {

    public static final SweepStrategy CONSERVATIVE =
            new SweepStrategy(Optional.of(SweeperStrategy.CONSERVATIVE), false, false);
    public static final SweepStrategy THOROUGH = new SweepStrategy(Optional.of(SweeperStrategy.THOROUGH), false, true);

    private final Optional<SweeperStrategy> sweeperStrategy;
    private final boolean mustCheckImmutableLockIfAllCellsReadAndPresent;

    // When we either read an empty value or perform reads like a range-scan, where we can't be certain of how many
    // values we expect.
    private final boolean mustCheckImmutableLockIfNonExhaustiveRead;

    private SweepStrategy(
            Optional<SweeperStrategy> sweeperStrategy,
            boolean mustCheckImmutableLockIfAllCellsReadAndPresent,
            boolean mustCheckImmutableLockIfNonExhaustiveRead) {
        this.sweeperStrategy = sweeperStrategy;
        this.mustCheckImmutableLockIfAllCellsReadAndPresent = mustCheckImmutableLockIfAllCellsReadAndPresent;
        this.mustCheckImmutableLockIfNonExhaustiveRead = mustCheckImmutableLockIfNonExhaustiveRead;
    }

    public Optional<SweeperStrategy> getSweeperStrategy() {
        return sweeperStrategy;
    }

    public boolean mustCheckImmutableLock(boolean allPossibleCellsReandAndPresent) {
        return allPossibleCellsReandAndPresent
                ? mustCheckImmutableLockIfAllCellsReadAndPresent
                : mustCheckImmutableLockIfNonExhaustiveRead;
    }

    public static SweepStrategy from(TableMetadataPersistence.SweepStrategy strategy, KeyValueService kvs) {
        return new SweepStrategy(
                sweeperBehaviour(strategy),
                mustCheckImmutableLockIfAllCellsReadAndPresent(strategy, kvs),
                mustCheckImmutableLockIfNonExhaustiveRead(strategy));
    }

    private static Optional<SweeperStrategy> sweeperBehaviour(TableMetadataPersistence.SweepStrategy strategy) {
        switch (strategy) {
            case CONSERVATIVE:
            case THOROUGH_MIGRATION:
                return Optional.of(SweeperStrategy.CONSERVATIVE);
            case THOROUGH:
                return Optional.of(SweeperStrategy.THOROUGH);
            case NOTHING:
                return Optional.empty();
        }
        throw new SafeIllegalStateException("Unknown case", SafeArg.of("strategy", strategy));
    }

    private static boolean mustCheckImmutableLockIfAllCellsReadAndPresent(
            TableMetadataPersistence.SweepStrategy strategy, KeyValueService kvs) {
        if (!kvs.sweepsEntriesInStrictlyNonDecreasingFashion()) {
            return mustCheckImmutableLockIfNonExhaustiveRead(strategy);
        }

        switch (strategy) {
            case CONSERVATIVE:
            case NOTHING:
            case THOROUGH_MIGRATION:
            case THOROUGH:
                return false;
        }
        throw new SafeIllegalStateException("Unknown case", SafeArg.of("strategy", strategy));
    }

    private static boolean mustCheckImmutableLockIfNonExhaustiveRead(TableMetadataPersistence.SweepStrategy strategy) {
        switch (strategy) {
            case CONSERVATIVE:
            case NOTHING:
                return false;
            case THOROUGH_MIGRATION:
            case THOROUGH:
                return true;
        }
        throw new SafeIllegalStateException("Unknown case", SafeArg.of("strategy", strategy));
    }
}

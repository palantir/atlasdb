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
package com.palantir.atlasdb.sweep;

import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.SpecialTimestampsSupplier;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.function.Function;

@SuppressWarnings("ImmutableEnumChecker")
public enum Sweeper {
    CONSERVATIVE(
            provider -> Math.min(provider.getUnreadableTimestamp(), provider.getImmutableTimestamp()),
            false,
            true,
            true),
    THOROUGH(SpecialTimestampsSupplier::getImmutableTimestamp, true, false, true),
    NO_OP(
            provider -> Math.min(provider.getUnreadableTimestamp(), provider.getImmutableTimestamp()),
            false,
            false,
            false);

    private final Function<SpecialTimestampsSupplier, Long> sweepTimestampSupplier;
    private final boolean shouldSweepLastCommitted;
    private final boolean shouldAddSentinels;
    private final boolean shouldDeleteCells;

    Sweeper(
            Function<SpecialTimestampsSupplier, Long> sweepTimestampSupplier,
            boolean shouldSweepLastCommitted,
            boolean shouldAddSentinels,
            boolean shouldDeleteCells) {
        this.sweepTimestampSupplier = sweepTimestampSupplier;
        this.shouldSweepLastCommitted = shouldSweepLastCommitted;
        this.shouldAddSentinels = shouldAddSentinels;
        this.shouldDeleteCells = shouldDeleteCells;
    }

    public boolean shouldSweepLastCommitted() {
        return shouldSweepLastCommitted;
    }

    public boolean shouldAddSentinels() {
        return shouldAddSentinels;
    }

    public boolean shouldDeleteCells() {
        return shouldDeleteCells;
    }

    public long getSweepTimestamp(SpecialTimestampsSupplier specialTimestampsSupplier) {
        return sweepTimestampSupplier.apply(specialTimestampsSupplier);
    }

    public static Sweeper of(SweeperStrategy sweepStrategy) {
        switch (sweepStrategy) {
            case CONSERVATIVE:
                return CONSERVATIVE;
            case THOROUGH:
                return THOROUGH;
            case NON_SWEEPABLE:
                return NO_OP;
        }
        throw new SafeIllegalStateException("Unknown sweep strategy", SafeArg.of("strategy", sweepStrategy));
    }

    public static Sweeper of(ShardAndStrategy shardStrategy) {
        return of(shardStrategy.strategy());
    }
}

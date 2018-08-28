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
package com.palantir.atlasdb.sweep;

import java.util.Optional;
import java.util.function.Function;

import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.SpecialTimestampsSupplier;

public enum Sweeper {
    CONSERVATIVE(provider -> Math.min(provider.getUnreadableTimestamp(), provider.getImmutableTimestamp()),
                 false,
                 true),
    THOROUGH(provider -> provider.getImmutableTimestamp(),
             true,
             false);

    private final Function<SpecialTimestampsSupplier, Long> sweepTimestampSupplier;
    private final boolean shouldSweepLastCommitted;
    private final boolean shouldAddSentinels;

    Sweeper(Function<SpecialTimestampsSupplier, Long> sweepTimestampSupplier,
            boolean shouldSweepLastCommitted, boolean shouldAddSentinels) {
        this.sweepTimestampSupplier = sweepTimestampSupplier;
        this.shouldSweepLastCommitted = shouldSweepLastCommitted;
        this.shouldAddSentinels = shouldAddSentinels;
    }

    public boolean shouldSweepLastCommitted() {
        return shouldSweepLastCommitted;
    }

    public boolean shouldAddSentinels() {
        return shouldAddSentinels;
    }

    public long getSweepTimestamp(SpecialTimestampsSupplier specialTimestampsSupplier) {
        return sweepTimestampSupplier.apply(specialTimestampsSupplier);
    }

    public static Optional<Sweeper> of(TableMetadataPersistence.SweepStrategy sweepStrategy) {
        switch (sweepStrategy) {
            case NOTHING:
                return Optional.empty();
            case CONSERVATIVE:
                return Optional.of(CONSERVATIVE);
            case THOROUGH:
                return Optional.of(THOROUGH);
            default:
                throw new IllegalArgumentException("Unknown sweep strategy: " + sweepStrategy);
        }
    }

    public static Sweeper of(ShardAndStrategy shardStrategy) {
        return of(shardStrategy.strategy())
                .orElseThrow(() -> new IllegalArgumentException("Unknown sweep strategy: " + shardStrategy.strategy()));
    }
}

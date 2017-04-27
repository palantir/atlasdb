/*
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
package com.palantir.atlasdb.sweep;

import java.util.Set;
import java.util.function.LongSupplier;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;

public enum Sweeper {
    CONSERVATIVE((unreadableTs, immutableTs) -> Math.min(unreadableTs.getAsLong(), immutableTs.getAsLong()),
                 ImmutableSet.of(Value.INVALID_VALUE_TIMESTAMP),
                 false,
                 true),
    THOROUGH((unreadableTs, immutableTs) -> immutableTs.getAsLong(),
             ImmutableSet.of(),
             true,
             false);

    private final SweepTimestampSupplier sweepTimestampSupplier;
    private final Set<Long> timestampsToIgnore;
    private final boolean shouldSweepLastCommitted;
    private final boolean shouldAddSentinels;

    Sweeper(SweepTimestampSupplier sweepTimestampSupplier, Set<Long> timestampsToIgnore,
            boolean shouldSweepLastCommitted, boolean shouldAddSentinels) {
        this.sweepTimestampSupplier = sweepTimestampSupplier;
        this.timestampsToIgnore = timestampsToIgnore;
        this.shouldSweepLastCommitted = shouldSweepLastCommitted;
        this.shouldAddSentinels = shouldAddSentinels;
    }

    public SweepTimestampSupplier getSweepTimestampSupplier() {
        return sweepTimestampSupplier;
    }

    public Set<Long> getTimestampsToIgnore() {
        return timestampsToIgnore;
    }

    public boolean shouldSweepLastCommitted() {
        return shouldSweepLastCommitted;
    }

    public boolean shouldAddSentinels() {
        return shouldAddSentinels;
    }

    public interface SweepTimestampSupplier {
        long getSweepTimestamp(LongSupplier unreadableTimestampSupplier, LongSupplier immutableTimestampSupplier);
    }

    public static Optional<Sweeper> of(TableMetadataPersistence.SweepStrategy sweepStrategy) {
        switch (sweepStrategy) {
            case NOTHING:
                return Optional.absent();
            case CONSERVATIVE:
                return Optional.of(CONSERVATIVE);
            case THOROUGH:
                return Optional.of(THOROUGH);
            default:
                throw new IllegalArgumentException("Unknown sweep strategy: " + sweepStrategy);
        }
    }

}

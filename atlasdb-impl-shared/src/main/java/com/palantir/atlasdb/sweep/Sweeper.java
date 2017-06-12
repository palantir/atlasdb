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
import java.util.function.LongSupplier;

import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;

public enum Sweeper {
    CONSERVATIVE((unreadableTs, immutableTs) -> Math.min(unreadableTs.getAsLong(), immutableTs.getAsLong()),
                 new long[] {Value.INVALID_VALUE_TIMESTAMP},
                 false,
                 true),
    THOROUGH((unreadableTs, immutableTs) -> immutableTs.getAsLong(),
             new long[] {},
             true,
             false);

    private final SweepTimestampSupplier sweepTimestampSupplier;
    private final long[] timestampsToIgnore;
    private final boolean shouldSweepLastCommitted;
    private final boolean shouldAddSentinels;

    Sweeper(SweepTimestampSupplier sweepTimestampSupplier, long[] timestampsToIgnore,
            boolean shouldSweepLastCommitted, boolean shouldAddSentinels) {
        this.sweepTimestampSupplier = sweepTimestampSupplier;
        this.timestampsToIgnore = timestampsToIgnore;
        this.shouldSweepLastCommitted = shouldSweepLastCommitted;
        this.shouldAddSentinels = shouldAddSentinels;
    }

    public SweepTimestampSupplier getSweepTimestampSupplier() {
        return sweepTimestampSupplier;
    }

    public long[] getTimestampsToIgnore() {
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
                return Optional.empty();
            case CONSERVATIVE:
                return Optional.of(CONSERVATIVE);
            case THOROUGH:
                return Optional.of(THOROUGH);
            default:
                throw new IllegalArgumentException("Unknown sweep strategy: " + sweepStrategy);
        }
    }

}

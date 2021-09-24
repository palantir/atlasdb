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
package com.palantir.atlasdb.cassandra;

import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;

public final class CassandraMutationTimestampProviders {
    private CassandraMutationTimestampProviders() {
        // factory
    }

    /**
     * As far as possible, users should switch to {@link #singleLongSupplierBacked(LongSupplier)} with a
     * fresh timestamp source from AtlasDB to promote better Cassandra compaction behaviour.
     *
     * @return {@link CassandraMutationTimestampProvider} which behaves in line with existing behaviour
     */
    public static CassandraMutationTimestampProvider legacyModeForTestsOnly() {
        return new CassandraMutationTimestampProvider() {
            @Override
            public long getSweepSentinelWriteTimestamp() {
                return 0;
            }

            @Override
            public long getRemoveTimestamp() {
                throw new UnsupportedOperationException("Can't be implemented");
            }

            @Override
            public LongUnaryOperator getDeletionTimestampOperatorForBatchDelete() {
                return deletionTimestamp -> deletionTimestamp + 1;
            }

            @Override
            public long getRangeTombstoneTimestamp(long maximumAtlasTimestampExclusive) {
                return maximumAtlasTimestampExclusive + 1;
            }
        };
    }

    public static CassandraMutationTimestampProvider singleLongSupplierBacked(LongSupplier longSupplier) {
        return new CassandraMutationTimestampProvider() {
            @Override
            public long getSweepSentinelWriteTimestamp() {
                return longSupplier.getAsLong();
            }

            @Override
            public long getRemoveTimestamp() {
                return longSupplier.getAsLong();
            }

            @Override
            public LongUnaryOperator getDeletionTimestampOperatorForBatchDelete() {
                long deletionTimestamp = longSupplier.getAsLong();
                return unused -> deletionTimestamp;
            }

            @Override
            public long getRangeTombstoneTimestamp(long maximumAtlasTimestampExclusive) {
                return longSupplier.getAsLong();
            }
        };
    }
}

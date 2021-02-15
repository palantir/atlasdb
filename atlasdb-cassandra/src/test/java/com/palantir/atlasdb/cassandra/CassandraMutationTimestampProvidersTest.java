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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongUnaryOperator;
import org.junit.Test;

public class CassandraMutationTimestampProvidersTest {
    private static final CassandraMutationTimestampProvider LEGACY_PROVIDER =
            CassandraMutationTimestampProviders.legacyModeForTestsOnly();

    private final AtomicLong timestamp = new AtomicLong(0);
    private final CassandraMutationTimestampProvider supplierBackedProvider =
            CassandraMutationTimestampProviders.singleLongSupplierBacked(timestamp::incrementAndGet);

    @Test
    public void legacyProviderWritesSweepSentinelsAtTimestampZero() {
        assertThat(LEGACY_PROVIDER.getSweepSentinelWriteTimestamp()).isEqualTo(0);
        assertThat(LEGACY_PROVIDER.getSweepSentinelWriteTimestamp()).isEqualTo(0);
    }

    @Test
    public void legacyProviderWritesTombstonesAtAtlasTimestampPlusOne() {
        assertThat(LEGACY_PROVIDER.getDeletionTimestampOperatorForBatchDelete().applyAsLong(1234))
                .isEqualTo(1234 + 1);
        assertThat(LEGACY_PROVIDER.getDeletionTimestampOperatorForBatchDelete().applyAsLong(12345678))
                .isEqualTo(12345678 + 1);
    }

    @Test
    public void legacyProviderWritesRangeTombstonesAtAtlasTimestampPlusOne() {
        assertThat(LEGACY_PROVIDER.getRangeTombstoneTimestamp(1234)).isEqualTo(1234 + 1);
        assertThat(LEGACY_PROVIDER.getRangeTombstoneTimestamp(12345678)).isEqualTo(12345678 + 1);
    }

    @Test
    public void supplierBackedProviderQueriesSupplierForSweepSentinelTimestamps() {
        assertThat(supplierBackedProvider.getSweepSentinelWriteTimestamp()).isEqualTo(1);
        assertThat(supplierBackedProvider.getSweepSentinelWriteTimestamp()).isEqualTo(2);
        assertThat(supplierBackedProvider.getSweepSentinelWriteTimestamp()).isEqualTo(3);
    }

    @Test
    public void supplierBackedProviderQueriesSupplierForDeletionTimestamps() {
        assertThat(supplierBackedProvider
                        .getDeletionTimestampOperatorForBatchDelete()
                        .applyAsLong(1234))
                .isEqualTo(1);
        assertThat(supplierBackedProvider
                        .getDeletionTimestampOperatorForBatchDelete()
                        .applyAsLong(12345678))
                .isEqualTo(2);
        assertThat(supplierBackedProvider
                        .getDeletionTimestampOperatorForBatchDelete()
                        .applyAsLong(314159265358979L))
                .isEqualTo(3);
    }

    @Test
    public void supplierBackedProviderOnlyInvokesSupplierOncePerDeletionOperator() {
        LongUnaryOperator deletionTimestamps = supplierBackedProvider.getDeletionTimestampOperatorForBatchDelete();
        assertThat(deletionTimestamps.applyAsLong(42)).isEqualTo(1);
        assertThat(deletionTimestamps.applyAsLong(9999)).isEqualTo(1);
        assertThat(deletionTimestamps.applyAsLong(1213141516171819L)).isEqualTo(1);
    }

    @Test
    public void supplierBackedProviderQueriesSupplierForRangeTombstoneTimestamps() {
        assertThat(supplierBackedProvider.getRangeTombstoneTimestamp(3141592)).isEqualTo(1);
        assertThat(supplierBackedProvider.getRangeTombstoneTimestamp(0)).isEqualTo(2);
        assertThat(supplierBackedProvider.getRangeTombstoneTimestamp(1L << 60)).isEqualTo(3);
    }
}

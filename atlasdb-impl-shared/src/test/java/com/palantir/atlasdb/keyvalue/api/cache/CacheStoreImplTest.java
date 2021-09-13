/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.codahale.metrics.Gauge;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.watch.Sequence;
import com.palantir.atlasdb.keyvalue.api.watch.StartTimestamp;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import io.vavr.collection.HashMap;
import io.vavr.collection.HashSet;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public final class CacheStoreImplTest {
    private static final StartTimestamp TIMESTAMP_1 = StartTimestamp.of(1L);
    private static final StartTimestamp TIMESTAMP_2 = StartTimestamp.of(22L);
    private static final double VALIDATION_PROBABILITY = 1.0;

    private final CacheMetrics metrics = mock(CacheMetrics.class);

    @Test
    public void updatesToSnapshotStoreReflectedInCacheStore() {
        SnapshotStore snapshotStore = new SnapshotStoreImpl();
        CacheStore cacheStore = new CacheStoreImpl(snapshotStore, VALIDATION_PROBABILITY, () -> {}, metrics, 100);

        cacheStore.createCache(TIMESTAMP_1);
        assertThat(cacheStore.getCache(TIMESTAMP_1)).isExactlyInstanceOf(NoOpTransactionScopedCache.class);

        snapshotStore.storeSnapshot(
                Sequence.of(5L),
                ImmutableSet.of(TIMESTAMP_2),
                ValueCacheSnapshotImpl.of(HashMap.empty(), HashSet.empty(), ImmutableSet.of()));
        cacheStore.createCache(TIMESTAMP_2);
        assertThat(cacheStore.getCache(TIMESTAMP_2)).isExactlyInstanceOf(ValidatingTransactionScopedCache.class);
        assertThat(getTransactionCacheInstanceCount()).isEqualTo(1);
    }

    @Test
    public void multipleCallsToGetReturnsTheSameCache() {
        SnapshotStore snapshotStore = new SnapshotStoreImpl();
        CacheStore cacheStore = new CacheStoreImpl(snapshotStore, VALIDATION_PROBABILITY, () -> {}, metrics, 100);
        snapshotStore.storeSnapshot(
                Sequence.of(5L),
                ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2),
                ValueCacheSnapshotImpl.of(HashMap.empty(), HashSet.empty(), ImmutableSet.of()));

        cacheStore.createCache(TIMESTAMP_1);
        cacheStore.createCache(TIMESTAMP_2);
        TransactionScopedCache cache1 = cacheStore.getCache(TIMESTAMP_1);
        TransactionScopedCache cache2 = cacheStore.getCache(TIMESTAMP_2);

        assertThat(cacheStore.getCache(TIMESTAMP_1)).isEqualTo(cache1).isNotEqualTo(cache2);
        assertThat(getTransactionCacheInstanceCount()).isEqualTo(2);
    }

    @Test
    public void cachesExceedingMaximumCountThrows() {
        SnapshotStore snapshotStore = new SnapshotStoreImpl();
        CacheStore cacheStore = new CacheStoreImpl(snapshotStore, VALIDATION_PROBABILITY, () -> {}, metrics, 1);

        StartTimestamp timestamp = StartTimestamp.of(22222L);
        snapshotStore.storeSnapshot(
                Sequence.of(5L),
                ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2, timestamp),
                ValueCacheSnapshotImpl.of(HashMap.empty(), HashSet.empty(), ImmutableSet.of()));

        cacheStore.createCache(TIMESTAMP_1);
        cacheStore.createCache(timestamp);
        assertThatThrownBy(() -> cacheStore.createCache(TIMESTAMP_2))
                .isExactlyInstanceOf(TransactionFailedRetriableException.class)
                .hasMessage("Exceeded maximum concurrent caches; transaction can be retried, but with caching "
                        + "disabled");
        assertThat(getTransactionCacheInstanceCount()).isEqualTo(2);
    }

    @Test
    public void getCacheDoesNotPersistAnything() {
        SnapshotStore snapshotStore = new SnapshotStoreImpl();
        CacheStore cacheStore = new CacheStoreImpl(snapshotStore, VALIDATION_PROBABILITY, () -> {}, metrics, 100);
        snapshotStore.storeSnapshot(
                Sequence.of(5L),
                ImmutableSet.of(TIMESTAMP_1),
                ValueCacheSnapshotImpl.of(HashMap.empty(), HashSet.empty(), ImmutableSet.of()));

        assertThat(cacheStore.getCache(TIMESTAMP_1)).isExactlyInstanceOf(NoOpTransactionScopedCache.class);
        cacheStore.createCache(TIMESTAMP_1);
        assertThat(cacheStore.getCache(TIMESTAMP_1)).isExactlyInstanceOf(ValidatingTransactionScopedCache.class);
        assertThat(getTransactionCacheInstanceCount()).isEqualTo(1);
    }

    @Test
    public void noOpCachesAreNotStored() {
        SnapshotStore snapshotStore = new SnapshotStoreImpl();
        CacheStore cacheStore = new CacheStoreImpl(snapshotStore, VALIDATION_PROBABILITY, () -> {}, metrics, 0);

        cacheStore.createCache(TIMESTAMP_1);
        cacheStore.createCache(TIMESTAMP_2);
        assertThat(cacheStore.getCache(TIMESTAMP_1)).isExactlyInstanceOf(NoOpTransactionScopedCache.class);
        assertThat(cacheStore.getCache(TIMESTAMP_2)).isExactlyInstanceOf(NoOpTransactionScopedCache.class);
        assertThat(getTransactionCacheInstanceCount()).isEqualTo(0);
    }

    @SuppressWarnings("unchecked")
    private int getTransactionCacheInstanceCount() {
        ArgumentCaptor<Gauge<Integer>> cacheInstanceCount = ArgumentCaptor.forClass(Gauge.class);
        verify(metrics).setTransactionCacheInstanceCountGauge(cacheInstanceCount.capture());
        return cacheInstanceCount.getValue().getValue();
    }
}

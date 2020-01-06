/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.palantir.atlasdb.cache.OffHeapTimestampCache.CacheDescriptor;
import com.palantir.atlasdb.offheap.ImmutableStoreNamespace;
import com.palantir.atlasdb.offheap.PersistentTimestampStore;
import com.palantir.atlasdb.offheap.PersistentTimestampStore.StoreNamespace;

public final class OffHeapTimeStampCacheTests {
    private static ExecutorService executorService;

    private PersistentTimestampStore persistentTimestampStore;

    @BeforeClass
    public static void classSetUp() {
        executorService = Executors.newCachedThreadPool();
    }

    @AfterClass
    public static void classTearDown() {
        executorService.shutdown();
    }

    @Before
    public void setUp() {
        persistentTimestampStore = mock(PersistentTimestampStore.class);
    }

    @Test
    public void testConcurrentClears() throws BrokenBarrierException, InterruptedException {
        CyclicBarrier mockVerificationBarrier = new CyclicBarrier(3);
        CyclicBarrier cyclicBarrier = new CyclicBarrier(2);
        when(persistentTimestampStore.createNamespace(any()))
                .thenAnswer(invocation -> {
                    cyclicBarrier.await();
                    return randomStoreNamespace();
                });

        CacheDescriptor cacheDescriptor = constructCacheDescriptor(0);

        OffHeapTimestampCache offHeapTimestampCache = constructOffHeapTimestampCache(cacheDescriptor);
        submitJob(offHeapTimestampCache::clear, mockVerificationBarrier);
        submitJob(offHeapTimestampCache::clear, mockVerificationBarrier);

        mockVerificationBarrier.await();
        verify(persistentTimestampStore, times(2)).dropNamespace(any());
    }

    @Test
    public void testSequentialWriters() {
        when(persistentTimestampStore.get(any(), any())).thenReturn(null).thenReturn(4L);

        CacheDescriptor cacheDescriptor = constructCacheDescriptor(0);

        OffHeapTimestampCache offHeapTimestampCache = constructOffHeapTimestampCache(cacheDescriptor);
        offHeapTimestampCache.putAlreadyCommittedTransaction(3L, 4L);
        offHeapTimestampCache.putAlreadyCommittedTransaction(3L, 4L);

        verify(persistentTimestampStore, times(1)).put(any(), any(), any());
        verify(persistentTimestampStore, times(2)).get(any(), any());
    }

    @Test
    public void testConcurrentWritersForSameKey() throws BrokenBarrierException, InterruptedException {
        CyclicBarrier mockVerificationBarrier = new CyclicBarrier(2);
        CyclicBarrier blockOnGet = new CyclicBarrier(2);
        CyclicBarrier blockSecondExit = new CyclicBarrier(2);

        when(persistentTimestampStore.get(any(), any()))
                .thenAnswer(invocation -> {
                    blockOnGet.await();
                    blockSecondExit.await();
                    return null;
                });

        CacheDescriptor cacheDescriptor = constructCacheDescriptor(0);
        OffHeapTimestampCache offHeapTimestampCache = constructOffHeapTimestampCache(cacheDescriptor);

        submitJob(
                () -> offHeapTimestampCache.putAlreadyCommittedTransaction(3L, 4L),
                mockVerificationBarrier);
        blockOnGet.await();
        offHeapTimestampCache.putAlreadyCommittedTransaction(3L, 4L);
        blockSecondExit.await();

        mockVerificationBarrier.await();
        verify(persistentTimestampStore, times(1)).put(any(), any(), any());
        verify(persistentTimestampStore, times(1)).get(any(), any());
    }

    @Test
    public void testConcurrentWritersForDifferentKey() throws BrokenBarrierException, InterruptedException {
        CyclicBarrier mockVerificationBarrier = new CyclicBarrier(2);
        CyclicBarrier blockOnGet = new CyclicBarrier(2);
        CyclicBarrier blockSecondExit = new CyclicBarrier(2);

        when(persistentTimestampStore.get(any(), any()))
                .thenAnswer(invocation -> {
                    blockOnGet.await();
                    blockSecondExit.await();
                    return null;
                })
                .thenReturn(null);

        CacheDescriptor cacheDescriptor = constructCacheDescriptor(0);
        OffHeapTimestampCache offHeapTimestampCache = constructOffHeapTimestampCache(cacheDescriptor);

        submitJob(() -> offHeapTimestampCache.putAlreadyCommittedTransaction(3L, 4L), mockVerificationBarrier);
        blockOnGet.await();
        offHeapTimestampCache.putAlreadyCommittedTransaction(1L, 2L);
        blockSecondExit.await();

        mockVerificationBarrier.await();
        verify(persistentTimestampStore, times(2)).put(any(), any(), any());
        verify(persistentTimestampStore, times(2)).get(any(), any());
    }

    @Test
    public void cacheFull() {
        CacheDescriptor cacheDescriptor = constructCacheDescriptor(10);
        StoreNamespace proposal = randomStoreNamespace();

        when(persistentTimestampStore.createNamespace(any())).thenReturn(proposal);
        when(persistentTimestampStore.get(any(), any())).thenReturn(null);

        OffHeapTimestampCache offHeapTimestampCache = constructOffHeapTimestampCache(cacheDescriptor);
        offHeapTimestampCache.putAlreadyCommittedTransaction(1L, 2L);

        verify(persistentTimestampStore, times(1)).dropNamespace(eq(cacheDescriptor.storeNamespace()));
        verify(persistentTimestampStore, times(1)).get(eq(cacheDescriptor.storeNamespace()), any());
        verify(persistentTimestampStore, times(1)).put(eq(proposal), any(), any());
    }

    @Test
    public void concurrentWritingToFullCache() throws InterruptedException, BrokenBarrierException {
        CyclicBarrier mockVerificationBarrier = new CyclicBarrier(3);
        CyclicBarrier creationBarrier = new CyclicBarrier(2);
        CountDownLatch swapLatch = new CountDownLatch(1);

        CacheDescriptor cacheDescriptor = constructCacheDescriptor(10);
        StoreNamespace firstProposal = randomStoreNamespace();
        StoreNamespace secondProposal = randomStoreNamespace();

        when(persistentTimestampStore.createNamespace(any()))
                .thenAnswer(invocation -> {
                    creationBarrier.await();
                    return firstProposal;
                })
                .thenAnswer(invocation -> {
                    creationBarrier.await();
                    swapLatch.await();
                    return secondProposal;
                });
        when(persistentTimestampStore.get(any(), any())).thenReturn(null);

        OffHeapTimestampCache offHeapTimestampCache = constructOffHeapTimestampCache(cacheDescriptor);
        submitJob(
                () -> {
                    offHeapTimestampCache.putAlreadyCommittedTransaction(1L, 2L);
                    swapLatch.countDown();
                },
                mockVerificationBarrier);
        submitJob(
                () -> {
                    offHeapTimestampCache.putAlreadyCommittedTransaction(3L, 4L);
                    swapLatch.countDown();
                },
                mockVerificationBarrier);
        swapLatch.await();

        mockVerificationBarrier.await();
        verify(persistentTimestampStore, times(1)).dropNamespace(eq(cacheDescriptor.storeNamespace()));
        verify(persistentTimestampStore, times(1)).dropNamespace(eq(secondProposal));
        verify(persistentTimestampStore, times(2)).get(eq(cacheDescriptor.storeNamespace()), any());
        verify(persistentTimestampStore, times(2)).put(eq(firstProposal), any(), any());
    }

    @Test
    public void sequentialReader() {
        CacheDescriptor cacheDescriptor = constructCacheDescriptor(10);
        when(persistentTimestampStore.get(any(), any())).thenReturn(null);

        OffHeapTimestampCache offHeapTimestampCache = constructOffHeapTimestampCache(cacheDescriptor);
        offHeapTimestampCache.getCommitTimestampIfPresent(1L);

        verify(persistentTimestampStore, times(1)).get(any(), any());
    }

    @Test
    public void concurrentReaderAndWriter() throws BrokenBarrierException, InterruptedException {
        CyclicBarrier mockVerificationBarrier = new CyclicBarrier(2);
        CyclicBarrier blockOnGet = new CyclicBarrier(2);
        CyclicBarrier blockSecondExit = new CyclicBarrier(2);

        when(persistentTimestampStore.get(any(), any()))
                .thenAnswer(invocation -> {
                    blockOnGet.await();
                    blockSecondExit.await();
                    return null;
                });

        CacheDescriptor cacheDescriptor = constructCacheDescriptor(0);
        OffHeapTimestampCache offHeapTimestampCache = constructOffHeapTimestampCache(cacheDescriptor);

        submitJob(
                () -> offHeapTimestampCache.putAlreadyCommittedTransaction(3L, 4L),
                mockVerificationBarrier);
        blockOnGet.await();
        assertThat(offHeapTimestampCache.getCommitTimestampIfPresent(3L)).isEqualTo(4L);
        blockSecondExit.await();

        mockVerificationBarrier.await();
        verify(persistentTimestampStore, times(1)).put(any(), any(), any());
        verify(persistentTimestampStore, times(1)).get(any(), any());
    }

    private OffHeapTimestampCache constructOffHeapTimestampCache(CacheDescriptor cacheDescriptor) {
        return new OffHeapTimestampCache(persistentTimestampStore, cacheDescriptor, 10);
    }

    private CacheDescriptor constructCacheDescriptor(int artificialCurrentSize) {
        return ImmutableCacheDescriptor.builder()
                .currentSize(new AtomicInteger(artificialCurrentSize))
                .storeNamespace(randomStoreNamespace())
                .build();
    }

    private StoreNamespace randomStoreNamespace() {
        return ImmutableStoreNamespace.builder()
                .humanReadableName("test")
                .uniqueName(UUID.randomUUID())
                .build();
    }

    private void submitJob(Runnable runnable, CyclicBarrier mockVerificationBarrier) {
        executorService.submit(() -> {
            runnable.run();
            mockVerificationBarrier.await();
            return null;
        });
    }
}

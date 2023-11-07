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

package com.palantir.atlasdb.keyvalue.api.watch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.ResilientLockWatchProxy;
import com.palantir.atlasdb.keyvalue.api.cache.CacheMetrics;
import com.palantir.atlasdb.keyvalue.api.cache.LockWatchValueScopingCache;
import com.palantir.atlasdb.transaction.api.TransactionFailedNonRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.logsafe.exceptions.SafeNullPointerException;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class ResilientLockWatchProxyTest {
    @Mock
    private LockWatchEventCache defaultCache;

    @Mock
    private LockWatchEventCache fallbackCache;

    @Mock
    private CacheMetrics metrics;

    private LockWatchEventCache proxyEventCache;

    @BeforeEach
    public void before() {
        proxyEventCache = ResilientLockWatchProxy.newEventCacheProxy(defaultCache, fallbackCache, metrics);
    }

    @Test
    public void valueCacheProxyThrowsIfDelegateNotSet() {
        LockWatchValueScopingCache fallbackCache = mock(LockWatchValueScopingCache.class);
        ResilientLockWatchProxy<LockWatchValueScopingCache> proxyFactory =
                ResilientLockWatchProxy.newValueCacheProxyFactory(fallbackCache, metrics);

        assertThatThrownBy(proxyFactory::newValueCacheProxy)
                .isExactlyInstanceOf(SafeNullPointerException.class)
                .hasMessage("Delegate cache must be set before creating proxy");
    }

    @Test
    public void valueCacheProxyAlsoFallsBackOnException() {
        LockWatchValueScopingCache defaultCache = mock(LockWatchValueScopingCache.class);
        LockWatchValueScopingCache fallbackCache = mock(LockWatchValueScopingCache.class);
        ResilientLockWatchProxy<LockWatchValueScopingCache> proxyFactory =
                ResilientLockWatchProxy.newValueCacheProxyFactory(fallbackCache, metrics);
        proxyFactory.setDelegate(defaultCache);
        LockWatchValueScopingCache proxyCache = proxyFactory.newValueCacheProxy();

        // Normal operation
        long timestamp = 1L;
        Set<Long> timestamps = ImmutableSet.of(timestamp);
        proxyCache.updateCacheWithCommitTimestampsInformation(timestamps);
        verify(defaultCache).updateCacheWithCommitTimestampsInformation(timestamps);
        verify(fallbackCache, never()).updateCacheWithCommitTimestampsInformation(any());

        // Failure
        when(defaultCache.getTransactionScopedCache(timestamp))
                .thenThrow(new TransactionFailedNonRetriableException(""));
        assertThatThrownBy(() -> proxyCache.getTransactionScopedCache(timestamp))
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class);
        verify(defaultCache).getTransactionScopedCache(timestamp);

        // Fallback operation
        proxyCache.processStartTransactions(timestamps);
        verify(fallbackCache).processStartTransactions(timestamps);
        verifyNoMoreInteractions(defaultCache);
        verifyNoMoreInteractions(fallbackCache);
    }

    @Test
    public void testCanDelegateIsEnabled() {
        when(defaultCache.isEnabled()).thenReturn(true);
        when(fallbackCache.isEnabled()).thenReturn(false);

        assertThat(proxyEventCache.isEnabled()).isTrue();
        verify(defaultCache).isEnabled();

        RuntimeException runtimeException = new RuntimeException();
        when(defaultCache.getCommitUpdate(anyLong())).thenThrow(runtimeException);
        assertThatThrownBy(() -> proxyEventCache.getCommitUpdate(0L))
                .hasCause(runtimeException)
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class);

        assertThat(proxyEventCache.isEnabled()).isFalse();
        verify(fallbackCache).isEnabled();

        verify(defaultCache).getCommitUpdate(0L);
        verifyNoMoreInteractions(defaultCache, fallbackCache);
    }

    @Test
    public void failCausesFallbackCacheToBeUsed() {
        RuntimeException runtimeException = new RuntimeException();
        when(defaultCache.getCommitUpdate(anyLong())).thenThrow(runtimeException);
        assertThatThrownBy(() -> proxyEventCache.getCommitUpdate(0L))
                .hasCause(runtimeException)
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class);

        proxyEventCache.lastKnownVersion();
        verify(fallbackCache).lastKnownVersion();
        verify(defaultCache, never()).lastKnownVersion();
    }

    @Test
    public void lockWatchFailedExceptionDoesNotCauseFallbackToBeUsed() {
        TransactionLockWatchFailedException lockWatchFailedException = new TransactionLockWatchFailedException("fail");
        when(defaultCache.getCommitUpdate(anyLong())).thenThrow(lockWatchFailedException);
        assertThatThrownBy(() -> proxyEventCache.getCommitUpdate(0L)).isEqualTo(lockWatchFailedException);

        proxyEventCache.lastKnownVersion();
        verify(defaultCache).lastKnownVersion();
        verify(fallbackCache, never()).lastKnownVersion();
    }

    @Test
    public void alreadyOnFallbackCausesExceptionToBeRethrown() {
        RuntimeException runtimeException = new RuntimeException();
        when(defaultCache.getCommitUpdate(anyLong())).thenThrow(runtimeException);
        when(fallbackCache.getCommitUpdate(anyLong())).thenThrow(runtimeException);
        assertThatThrownBy(() -> proxyEventCache.getCommitUpdate(0L))
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasCause(runtimeException);
        assertThatThrownBy(() -> proxyEventCache.getCommitUpdate(0L))
                .isExactlyInstanceOf(SafeRuntimeException.class)
                .hasCause(runtimeException)
                .hasMessage("Fallback cache threw an exception");
    }
}

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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.cache.LockWatchValueScopingCache;
import com.palantir.atlasdb.timelock.api.LockWatchRequest;
import com.palantir.lock.client.NamespacedConjureLockWatchingService;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchReferences.LockWatchReference;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class LockWatchManagerImplTest {

    @Mock
    private LockWatchEventCache lockWatchEventCache;

    @Mock
    private LockWatchValueScopingCache valueScopingCache;

    @Mock
    private NamespacedConjureLockWatchingService lockWatchingService;

    @Mock
    private LockWatchReference lockWatchReference1;

    @Mock
    private LockWatchReference lockWatchReference2;

    @Mock
    private LockWatchReference fromSchema;

    private LockWatchManagerInternal manager;

    @Before
    public void before() {
        manager = new LockWatchManagerImpl(
                ImmutableSet.of(fromSchema), lockWatchEventCache, valueScopingCache, lockWatchingService);
    }

    @Test
    public void testDelegatesIsEnabled() {
        when(lockWatchEventCache.isEnabled()).thenReturn(false);
        assertThat(manager.isEnabled()).isFalse();
    }

    @Test
    public void onlyWatchCurrentWatches() {
        Set<LockWatchReference> firstReferences = ImmutableSet.of(lockWatchReference1, lockWatchReference2);
        manager.registerPreciselyWatches(firstReferences);
        // at least once as a background task also sends a startWatching request periodically, and this can race in the
        // test.
        verify(lockWatchingService, atLeastOnce())
                .startWatching(LockWatchRequest.builder()
                        .references(fromSchema)
                        .addAllReferences(firstReferences)
                        .build());
        manager.registerPreciselyWatches(ImmutableSet.of(lockWatchReference1));
        verify(lockWatchingService, atLeastOnce())
                .startWatching(LockWatchRequest.builder()
                        .references(ImmutableSet.of(fromSchema, lockWatchReference1))
                        .build());
    }

    @Test
    public void removeTransactionStateTest() {
        manager.removeTransactionStateFromCache(1L);
        verify(lockWatchEventCache).removeTransactionStateFromCache(1L);
        verify(valueScopingCache).removeTransactionState(1L);
    }

    @Test
    public void createTransactionScopedCacheTest() {
        manager.getOrCreateTransactionScopedCache(1L);
        verify(valueScopingCache).getOrCreateTransactionScopedCache(1L);
        verifyNoMoreInteractions(lockWatchEventCache);
    }
}

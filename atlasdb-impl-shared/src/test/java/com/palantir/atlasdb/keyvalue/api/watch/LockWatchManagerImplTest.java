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
import static org.mockito.Mockito.when;

import com.palantir.lock.client.NamespacedConjureLockWatchingService;
import com.palantir.lock.watch.LockWatchEventCache;
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
    private NamespacedConjureLockWatchingService lockWatchingService;

    private LockWatchManager manager;

    @Before
    public void before() {
        manager = new LockWatchManagerImpl(lockWatchEventCache, lockWatchingService);
    }

    @Test
    public void testDelegatesIsEnabled() {
        when(lockWatchEventCache.isEnabled()).thenReturn(false);
        assertThat(manager.isEnabled()).isFalse();
    }
}

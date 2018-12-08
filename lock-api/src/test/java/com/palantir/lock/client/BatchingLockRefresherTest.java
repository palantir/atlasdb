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

package com.palantir.lock.client;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Set;
import java.util.UUID;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;

public class BatchingLockRefresherTest {
    private static final LockToken TOKEN_1 = LockToken.of(UUID.randomUUID());
    private static final LockToken TOKEN_2 = LockToken.of(UUID.randomUUID());
    private static final Set<LockToken> TOKENS = ImmutableSet.of(TOKEN_1, TOKEN_2);

    private final TimelockService timelockService = mock(TimelockService.class);
    private final BatchingLockRefresher lockRefresher = BatchingLockRefresher.create(timelockService);

    @Test
    public void returnsRefreshedTokens() {
        when(timelockService.refreshLockLeases(any())).thenReturn(TOKENS);

        Set<LockToken> refreshedToken = lockRefresher.refreshLockLeases(TOKENS);
        assertEquals(TOKENS, refreshedToken);
    }

}
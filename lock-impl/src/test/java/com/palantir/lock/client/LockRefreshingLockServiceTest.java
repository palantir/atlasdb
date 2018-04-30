/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.lock.client;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockRequest.Builder;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.impl.LockServiceImpl;

public class LockRefreshingLockServiceTest {
    private LockService server;
    private LockDescriptor lock1;

    @Before public void setUp() {
        server = LockRefreshingLockService.create(LockServiceImpl.create(LockServerOptions.builder()
                .isStandaloneServer(false)
                .build()));
        lock1 = StringLockDescriptor.of("lock1");
    }

    @Test
    public void testSimpleRefresh() throws InterruptedException {
        Builder builder = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.WRITE));
        builder.timeoutAfter(SimpleTimeDuration.of(5, TimeUnit.SECONDS));
        LockResponse lock = server.lockWithFullLockResponse(LockClient.ANONYMOUS, builder.build());
        Thread.sleep(10000);
        Set<HeldLocksToken> refreshTokens = server.refreshTokens(ImmutableList.of(lock.getToken()));
        Assert.assertEquals(1, refreshTokens.size());
    }
}

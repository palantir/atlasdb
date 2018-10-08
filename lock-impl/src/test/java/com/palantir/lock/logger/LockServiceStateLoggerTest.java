/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.lock.logger;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockCollections;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.impl.ClientAwareReadWriteLock;
import com.palantir.lock.impl.LockClientIndices;
import com.palantir.lock.impl.LockServerLock;
import com.palantir.lock.impl.LockServiceImpl;

public class LockServiceStateLoggerTest {

    private final ConcurrentMap<HeldLocksToken, LockServiceImpl.HeldLocks<HeldLocksToken>> heldLocksTokenMap =
            new MapMaker().makeMap();

    private final SetMultimap<LockClient, LockRequest> outstandingLockRequestMultimap =
            Multimaps.synchronizedSetMultimap(HashMultimap.<LockClient, LockRequest>create());

    @Before
    public void setUp() throws Exception {
        LockClient clientA = LockClient.of("Client A");
        LockClient clientB = LockClient.of("Client B");

        LockDescriptor descriptor1 = StringLockDescriptor.of("logger-lock");
        LockDescriptor descriptor2 = StringLockDescriptor.of("logger-BBB");

        LockRequest request1 = LockRequest.builder(LockCollections.of(ImmutableSortedMap.of(descriptor1, LockMode.WRITE)))
                .blockForAtMost(SimpleTimeDuration.of(1000, TimeUnit.MILLISECONDS))
                .build();
        LockRequest request2 = LockRequest.builder(LockCollections.of(ImmutableSortedMap.of(descriptor2, LockMode.WRITE)))
                .blockForAtMost(SimpleTimeDuration.of(1000, TimeUnit.MILLISECONDS))
                .build();

        outstandingLockRequestMultimap.put(clientA, request1);
        outstandingLockRequestMultimap.put(clientB, request2);
        outstandingLockRequestMultimap.put(clientA, request2);

        HeldLocksToken token = getFakeHeldLocksToken("client A", "Fake thread", new BigInteger("1"));
        HeldLocksToken token2 = getFakeHeldLocksToken("client B", "Fake thread 2", new BigInteger("2"));

        Map<ClientAwareReadWriteLock, LockMode> locks = Maps.newLinkedHashMap();
        locks.put(new LockServerLock(StringLockDescriptor.of("logger-lock-2"), new LockClientIndices()), LockMode.WRITE);
        locks.put(new LockServerLock(StringLockDescriptor.of("logger-lock"), new LockClientIndices()), LockMode.READ);

        heldLocksTokenMap.putIfAbsent(token, LockServiceImpl.HeldLocks.of(token, LockCollections.of(locks)));

        Map<ClientAwareReadWriteLock, LockMode> locks2 = Maps.newLinkedHashMap();
        locks2.put(new LockServerLock(StringLockDescriptor.of("logger-lock-3"), new LockClientIndices()), LockMode.WRITE);
        locks2.put(new LockServerLock(StringLockDescriptor.of("logger-lock-4"), new LockClientIndices()), LockMode.READ);
        heldLocksTokenMap.putIfAbsent(token2, LockServiceImpl.HeldLocks.of(token2, LockCollections.of(locks2)));
    }

    private HeldLocksToken getFakeHeldLocksToken(String clientName, String requestingThread, BigInteger tokenId) {
        LockDescriptor descriptor1 = StringLockDescriptor.of("123");
        ImmutableSortedMap.Builder<LockDescriptor, LockMode> builder =
                ImmutableSortedMap.naturalOrder();
        builder.put(descriptor1, LockMode.WRITE);

        return new HeldLocksToken(tokenId, LockClient.of(clientName),
                System.currentTimeMillis(), System.currentTimeMillis(),
                LockCollections.of(builder.build()),
                LockRequest.DEFAULT_LOCK_TIMEOUT, 0L, requestingThread);
    }

    @Test
    public void testLocksLogging() throws Exception {
        LockServiceStateLogger logger = new LockServiceStateLogger(
                heldLocksTokenMap,
                outstandingLockRequestMultimap,
                ImmutableMap.of(),
                LockServiceLoggerTestUtils.TEST_LOG_STATE_DIR);
        logger.logLocks();
    }

    @After
    public void after() throws IOException {
        LockServiceLoggerTestUtils.cleanUpLogStateDir();
    }
}

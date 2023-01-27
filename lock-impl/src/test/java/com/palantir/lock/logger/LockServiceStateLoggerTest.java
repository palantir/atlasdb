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
package com.palantir.lock.logger;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockCollections;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.SortedLockCollection;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.impl.ClientAwareReadWriteLock;
import com.palantir.lock.impl.LockClientIndices;
import com.palantir.lock.impl.LockServerLock;
import com.palantir.lock.impl.LockServiceImpl;
import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;

public class LockServiceStateLoggerTest {

    private static final ConcurrentMap<HeldLocksToken, LockServiceImpl.HeldLocks<HeldLocksToken>> heldLocksTokenMap =
            new MapMaker().makeMap();

    private static final SetMultimap<LockClient, LockRequest> outstandingLockRequestMultimap =
            Multimaps.synchronizedSetMultimap(HashMultimap.create());

    private static final Map<LockDescriptor, ClientAwareReadWriteLock> syncStateMap = new HashMap<>();

    private static final LockDescriptor DESCRIPTOR_1 = StringLockDescriptor.of("logger-lock");
    private static final LockDescriptor DESCRIPTOR_2 = StringLockDescriptor.of("logger-AAA");

    private static LogState loggedState;

    @BeforeClass
    public static void setUp() {
        LockClient clientA = LockClient.of("Client A");
        LockClient clientB = LockClient.of("Client B");

        LockRequest request1 = LockRequest.builder(
                        LockCollections.of(ImmutableSortedMap.of(DESCRIPTOR_1, LockMode.WRITE)))
                .blockForAtMost(SimpleTimeDuration.of(1000, TimeUnit.MILLISECONDS))
                .build();
        LockRequest request2 = LockRequest.builder(
                        LockCollections.of(ImmutableSortedMap.of(DESCRIPTOR_2, LockMode.WRITE)))
                .blockForAtMost(SimpleTimeDuration.of(1000, TimeUnit.MILLISECONDS))
                .build();

        outstandingLockRequestMultimap.put(clientA, request1);
        outstandingLockRequestMultimap.put(clientB, request2);
        outstandingLockRequestMultimap.put(clientA, request2);

        HeldLocksToken token = LockServiceTestUtils.getFakeHeldLocksToken(
                "client A", "Fake thread", new BigInteger("1"), "held-lock-1", "logger-lock");
        HeldLocksToken token2 = LockServiceTestUtils.getFakeHeldLocksToken(
                "client B", "Fake thread 2", new BigInteger("2"), "held-lock-2", "held-lock-3");

        heldLocksTokenMap.putIfAbsent(token, LockServiceImpl.HeldLocks.of(token, LockCollections.of()));
        heldLocksTokenMap.putIfAbsent(token2, LockServiceImpl.HeldLocks.of(token2, LockCollections.of()));

        LockServerLock lock1 = new LockServerLock(DESCRIPTOR_1, new LockClientIndices());
        syncStateMap.put(DESCRIPTOR_1, lock1);
        LockServerLock lock2 = new LockServerLock(DESCRIPTOR_2, new LockClientIndices());
        lock2.get(clientA, LockMode.WRITE).lock();
        syncStateMap.put(DESCRIPTOR_2, lock2);

        String lockStats = "test lock stats";
        LockServiceStateLogger logger =
                new LockServiceStateLogger(heldLocksTokenMap, outstandingLockRequestMultimap, syncStateMap, lockStats);
        loggedState = logger.logLocks();
    }

    @Test
    public void testDescriptors() {
        Map<ObfuscatedLockDescriptor, LockDescriptor> lockDescriptorMapping = loggedState.getLockDescriptorMapping();
        Set<LockDescriptor> allDescriptors = getAllDescriptors();
        assertThat(lockDescriptorMapping.values()).containsExactlyInAnyOrderElementsOf(allDescriptors);
    }

    @Test
    public void testLockState() {
        Set<LockDescriptor> loggedOutstandingRequests = loggedState.getOutstandingRequests().stream()
                .map(request -> loggedState.getLockDescriptorMapping().get(request.getLockDescriptor()))
                .collect(Collectors.toSet());
        Set<LockDescriptor> outstandingDescriptors = getOutstandingDescriptors();
        assertThat(outstandingDescriptors).containsExactlyInAnyOrderElementsOf(loggedOutstandingRequests);

        Set<LockDescriptor> loggedHeldLocks = loggedState.getHeldLocks().keySet().stream()
                .map(obfuscatedLockDescriptor ->
                        loggedState.getLockDescriptorMapping().get(obfuscatedLockDescriptor))
                .collect(Collectors.toSet());
        Set<LockDescriptor> heldDescriptors = getHeldDescriptors();
        assertThat(heldDescriptors).containsExactlyInAnyOrderElementsOf(loggedHeldLocks);
    }

    @Test
    public void testSafelog() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        String outstandingReqSerialized = mapper.writeValueAsString(loggedState.getOutstandingRequests());
        assertDescriptorsNotPresentInString(outstandingReqSerialized);
        String heldLocksSerialized = mapper.writeValueAsString(loggedState.getHeldLocks());
        assertDescriptorsNotPresentInString(heldLocksSerialized);
        String syncStateSerialized = mapper.writeValueAsString(loggedState.getSyncState());
        assertDescriptorsNotPresentInString(syncStateSerialized);
        String synthesizedStateSerialized = mapper.writeValueAsString(loggedState.getSynthesizedRequestState());
        assertDescriptorsNotPresentInString(synthesizedStateSerialized);
    }

    @Test
    public void testSyncState() {
        Set<LockDescriptor> loggedSyncStateDescriptors = loggedState.getSyncState().keySet().stream()
                .map(lockDescriptor -> loggedState.getLockDescriptorMapping().get(lockDescriptor))
                .collect(Collectors.toSet());
        assertThat(getSyncStateDescriptors()).containsExactlyInAnyOrderElementsOf(loggedSyncStateDescriptors);
    }

    @Test
    public void testSynthesizedRequestState() {
        assertThat(loggedState.getSynthesizedRequestState()).hasSameSizeAs(getSyncStateDescriptors());
    }

    private static void assertDescriptorsNotPresentInString(String serialized) {
        assertThat(serialized).doesNotContain(DESCRIPTOR_1.getLockIdAsString());
        assertThat(serialized).doesNotContain(DESCRIPTOR_2.getLockIdAsString());
    }

    private Set<LockDescriptor> getAllDescriptors() {
        Set<LockDescriptor> allDescriptors = new HashSet<>();

        allDescriptors.addAll(getOutstandingDescriptors());
        allDescriptors.addAll(getHeldDescriptors());

        return allDescriptors;
    }

    private Set<LockDescriptor> getHeldDescriptors() {
        return heldLocksTokenMap.values().stream()
                .map(LockServiceImpl.HeldLocks::getRealToken)
                .map(HeldLocksToken::getLockDescriptors)
                .flatMap(SortedLockCollection::stream)
                .collect(Collectors.toSet());
    }

    private Set<LockDescriptor> getOutstandingDescriptors() {
        return outstandingLockRequestMultimap.values().stream()
                .map(LockRequest::getLockDescriptors)
                .flatMap(SortedLockCollection::stream)
                .collect(Collectors.toSet());
    }

    private Set<LockDescriptor> getSyncStateDescriptors() {
        return syncStateMap.keySet();
    }
}

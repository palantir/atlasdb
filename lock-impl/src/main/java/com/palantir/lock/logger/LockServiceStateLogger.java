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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockRequest;
import com.palantir.lock.impl.ClientAwareReadWriteLock;
import com.palantir.lock.impl.LockServerLock;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.lock.impl.LockServiceStateDebugger;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class LockServiceStateLogger {
    private static SafeLogger log = SafeLoggerFactory.get(LockServiceStateLogger.class);

    private final LockDescriptorMapper lockDescriptorMapper = new LockDescriptorMapper();

    private final ConcurrentMap<HeldLocksToken, LockServiceImpl.HeldLocks<HeldLocksToken>> heldLocks;
    private final Map<LockClient, Set<LockRequest>> outstandingLockRequests;
    private final Map<LockDescriptor, ClientAwareReadWriteLock> descriptorToLockMap;

    public LockServiceStateLogger(
            ConcurrentMap<HeldLocksToken, LockServiceImpl.HeldLocks<HeldLocksToken>> heldLocksTokenMap,
            SetMultimap<LockClient, LockRequest> outstandingLockRequestMultimap,
            Map<LockDescriptor, ClientAwareReadWriteLock> descriptorToLockMap) {
        this.heldLocks = heldLocksTokenMap;
        this.outstandingLockRequests = Multimaps.asMap(outstandingLockRequestMultimap);
        this.descriptorToLockMap = descriptorToLockMap;
    }

    public LogState logLocks() {
        List<SimpleLockRequestsWithSameDescriptor> generatedOutstandingRequests =
                generateOutstandingLocks(outstandingLockRequests);
        Map<ObfuscatedLockDescriptor, SimpleTokenInfo> generatedHeldLocks = generateHeldLocks(heldLocks);
        Map<ObfuscatedLockDescriptor, String> generatedSyncState = generateSyncState(descriptorToLockMap);
        Map<String, List<SanitizedLockRequestProgress>> synthesizedRequestState =
                synthesizeRequestState(outstandingLockRequests, descriptorToLockMap);

        LogState state = ImmutableLogState.builder()
                .outstandingRequests(generatedOutstandingRequests)
                .heldLocks(generatedHeldLocks)
                .syncState(generatedSyncState)
                .synthesizedRequestState(synthesizedRequestState)
                .lockDescriptorMapping(lockDescriptorMapper.getReversedMapper())
                .build();
        state.logTo(log);
        return state;
    }

    private Map<String, List<SanitizedLockRequestProgress>> synthesizeRequestState(
            Map<LockClient, Set<LockRequest>> outstandingLockRequests,
            Map<LockDescriptor, ClientAwareReadWriteLock> descriptorToLockMap) {
        LockServiceStateDebugger debugger = new LockServiceStateDebugger(outstandingLockRequests, descriptorToLockMap);
        Multimap<LockClient, LockServiceStateDebugger.LockRequestProgress> progressMultimap =
                debugger.getSuspectedLockProgress();

        return progressMultimap.asMap().entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().toString(), entry -> entry.getValue().stream()
                        .map(lockRequestProgress -> SanitizedLockRequestProgress.create(
                                lockRequestProgress,
                                lockDescriptorMapper,
                                entry.getKey().toString()))
                        .collect(Collectors.toList())));
    }

    private List<SimpleLockRequestsWithSameDescriptor> generateOutstandingLocks(
            Map<LockClient, Set<LockRequest>> outstandingLockRequestsMap) {
        Map<ObfuscatedLockDescriptor, ImmutableSimpleLockRequestsWithSameDescriptor.Builder> outstandingRequestMap =
                new HashMap<>();

        outstandingLockRequestsMap.forEach((client, requestSet) -> {
            if (requestSet != null) {
                ImmutableSet<LockRequest> lockRequests = ImmutableSet.copyOf(requestSet);
                lockRequests.forEach(lockRequest -> {
                    List<SimpleLockRequest> requestList = getDescriptorSimpleRequestMap(client, lockRequest);
                    requestList.forEach(request -> {
                        outstandingRequestMap.putIfAbsent(
                                request.getLockDescriptor(),
                                ImmutableSimpleLockRequestsWithSameDescriptor.builder()
                                        .lockDescriptor(request.getLockDescriptor()));
                        outstandingRequestMap.get(request.getLockDescriptor()).addLockRequests(request);
                    });
                });
            }
        });
        List<SimpleLockRequestsWithSameDescriptor> aggregatedRequests = new ArrayList<>();
        outstandingRequestMap.forEach((_descriptor, builder) -> aggregatedRequests.add(builder.build()));

        return sortOutstandingRequests(aggregatedRequests);
    }

    private List<SimpleLockRequestsWithSameDescriptor> sortOutstandingRequests(
            Collection<SimpleLockRequestsWithSameDescriptor> outstandingRequestsByDescriptor) {

        List<SimpleLockRequestsWithSameDescriptor> sortedEntries = new ArrayList<>(outstandingRequestsByDescriptor);
        sortedEntries.sort((o1, o2) -> Integer.compare(o2.getLockRequestsCount(), o1.getLockRequestsCount()));
        return sortedEntries;
    }

    private Map<ObfuscatedLockDescriptor, SimpleTokenInfo> generateHeldLocks(
            ConcurrentMap<HeldLocksToken, LockServiceImpl.HeldLocks<HeldLocksToken>> heldLocksTokenMap) {
        Map<ObfuscatedLockDescriptor, SimpleTokenInfo> mappedLocksToToken = new HashMap<>();
        heldLocksTokenMap
                .values()
                .forEach(locks -> mappedLocksToToken.putAll(getDescriptorToTokenMap(locks.getRealToken())));

        return mappedLocksToToken;
    }

    private Map<ObfuscatedLockDescriptor, String> generateSyncState(
            Map<LockDescriptor, ClientAwareReadWriteLock> descriptorToLockMap) {
        return descriptorToLockMap.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> lockDescriptorMapper.getDescriptorMapping(entry.getKey()),
                        entry -> ((LockServerLock) entry.getValue()).toSanitizedString()));
    }

    private List<SimpleLockRequest> getDescriptorSimpleRequestMap(LockClient client, LockRequest request) {
        return request.getLocks().stream()
                .map(lock -> SimpleLockRequest.of(
                        request,
                        this.lockDescriptorMapper.getDescriptorMapping(lock.getLockDescriptor()),
                        lock.getLockMode(),
                        client.getClientId()))
                .collect(Collectors.toList());
    }

    private Map<ObfuscatedLockDescriptor, SimpleTokenInfo> getDescriptorToTokenMap(HeldLocksToken realToken) {
        Map<ObfuscatedLockDescriptor, SimpleTokenInfo> lockToLockInfo = new HashMap<>();

        realToken
                .getLocks()
                .forEach(lock -> lockToLockInfo.put(
                        this.lockDescriptorMapper.getDescriptorMapping(lock.getLockDescriptor()),
                        SimpleTokenInfo.of(realToken, lock.getLockMode())));
        return lockToLockInfo;
    }
}

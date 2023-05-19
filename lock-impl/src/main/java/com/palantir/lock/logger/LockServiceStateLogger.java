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
import com.palantir.lock.impl.LockServiceImpl.HeldLocks;
import com.palantir.lock.impl.LockServiceStateDebugger;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class LockServiceStateLogger {
    private static final SafeLogger log = SafeLoggerFactory.get(LockServiceStateLogger.class);

    private final LockDescriptorMapper lockDescriptorMapper = new LockDescriptorMapper();

    private final ConcurrentMap<HeldLocksToken, LockServiceImpl.HeldLocks<HeldLocksToken>> heldLocks;
    private final Map<LockClient, Set<LockRequest>> outstandingLockRequests;
    private final Map<LockDescriptor, ClientAwareReadWriteLock> descriptorToLockMap;

    @Safe
    private final String lockStats;

    public LockServiceStateLogger(
            ConcurrentMap<HeldLocksToken, HeldLocks<HeldLocksToken>> heldLocksTokenMap,
            SetMultimap<LockClient, LockRequest> outstandingLockRequestMultimap,
            Map<LockDescriptor, ClientAwareReadWriteLock> descriptorToLockMap,
            @Safe String lockStats) {
        this.heldLocks = heldLocksTokenMap;
        this.outstandingLockRequests = Multimaps.asMap(outstandingLockRequestMultimap);
        this.descriptorToLockMap = descriptorToLockMap;
        this.lockStats = lockStats;
    }

    public LogState logLocks() {
        List<SimpleLockRequestsWithSameDescriptor> generatedOutstandingRequests =
                generateOutstandingLocks(outstandingLockRequests);
        Map<ObfuscatedLockDescriptor, SimpleTokenInfo> generatedHeldLocks = generateHeldLocks(heldLocks);
        Map<ObfuscatedLockDescriptor, String> generatedSyncState = generateSyncState(descriptorToLockMap);
        Map<ClientId, List<SanitizedLockRequestProgress>> synthesizedRequestState =
                synthesizeRequestState(outstandingLockRequests, descriptorToLockMap);

        LogState state = ImmutableLogState.builder()
                .outstandingRequests(generatedOutstandingRequests)
                .heldLocks(generatedHeldLocks)
                .syncState(generatedSyncState)
                .synthesizedRequestState(synthesizedRequestState)
                .lockDescriptorMapping(lockDescriptorMapper.getReversedMapper())
                .lockStats(lockStats)
                .build();
        state.logTo(log);
        return state;
    }

    private Map<ClientId, List<SanitizedLockRequestProgress>> synthesizeRequestState(
            Map<LockClient, Set<LockRequest>> outstandingLockRequests,
            Map<LockDescriptor, ClientAwareReadWriteLock> descriptorToLockMap) {
        LockServiceStateDebugger debugger = new LockServiceStateDebugger(outstandingLockRequests, descriptorToLockMap);
        Multimap<LockClient, LockServiceStateDebugger.LockRequestProgress> progressMultimap =
                debugger.getSuspectedLockProgress();

        return progressMultimap.asMap().entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> ClientId.of(entry.getKey().getClientId()), entry -> entry.getValue().stream()
                                .map(lockRequestProgress -> SanitizedLockRequestProgress.create(
                                        lockRequestProgress,
                                        this.lockDescriptorMapper,
                                        ClientId.of(entry.getKey().getClientId())))
                                .collect(Collectors.toList())));
    }

    private List<SimpleLockRequestsWithSameDescriptor> generateOutstandingLocks(
            Map<LockClient, Set<LockRequest>> outstandingLockRequestsMap) {
        Map<ObfuscatedLockDescriptor, ImmutableSimpleLockRequestsWithSameDescriptor.Builder> outstandingRequestMap =
                new HashMap<>();

        outstandingLockRequestsMap.forEach((client, requestSet) -> {
            if (requestSet != null) {
                ImmutableSet.copyOf(requestSet).forEach(lockRequest -> lockRequest.getLocks().stream()
                        .map(lock -> SimpleLockRequest.of(
                                lockRequest,
                                this.lockDescriptorMapper.getDescriptorMapping(lock.getLockDescriptor()),
                                lock.getLockMode(),
                                ClientId.of(client.getClientId())))
                        .forEach(request -> outstandingRequestMap
                                .computeIfAbsent(
                                        request.getLockDescriptor(),
                                        obfuscatedLockDescriptor ->
                                                ImmutableSimpleLockRequestsWithSameDescriptor.builder()
                                                        .lockDescriptor(obfuscatedLockDescriptor))
                                .addLockRequests(request)));
            }
        });

        return outstandingRequestMap.values().stream()
                .map(ImmutableSimpleLockRequestsWithSameDescriptor.Builder::build)
                .sorted(Comparator.comparing(SimpleLockRequestsWithSameDescriptor::getLockRequestsCount)
                        .reversed())
                .collect(Collectors.toList());
    }

    private Map<ObfuscatedLockDescriptor, SimpleTokenInfo> generateHeldLocks(
            ConcurrentMap<HeldLocksToken, LockServiceImpl.HeldLocks<HeldLocksToken>> heldLocksTokenMap) {
        return heldLocksTokenMap.values().stream()
                .flatMap(locks -> locks.getRealToken().getLocks().stream()
                        .map(lock -> Map.entry(
                                this.lockDescriptorMapper.getDescriptorMapping(lock.getLockDescriptor()),
                                SimpleTokenInfo.of(locks.getRealToken(), lock.getLockMode()))))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    private Map<ObfuscatedLockDescriptor, String> generateSyncState(
            Map<LockDescriptor, ClientAwareReadWriteLock> descriptorToLockMap) {
        return descriptorToLockMap.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> lockDescriptorMapper.getDescriptorMapping(entry.getKey()),
                        entry -> ((LockServerLock) entry.getValue()).toSanitizedString()));
    }
}

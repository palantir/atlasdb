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

package com.palantir.atlasdb.timelock.lock;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.debug.ImmutableLockDiagnosticInfo;
import com.palantir.atlasdb.debug.LockDiagnosticConfig;
import com.palantir.atlasdb.debug.LockDiagnosticInfo;
import com.palantir.atlasdb.debug.LockInfo;
import com.palantir.atlasdb.debug.LockState;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.LockDescriptor;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This should be removed once PDS-95791 is complete.
 * @deprecated Remove this once PDS-95791 is resolved.
 */
@Deprecated
public class LockDiagnosticCollector implements LockEvents {

    private static final Logger log = LoggerFactory.getLogger(LockDiagnosticCollector.class);

    private final Cache<UUID, Optional<LockInfo>> cache;

    LockDiagnosticCollector(LockDiagnosticConfig policyConfig) {
        this.cache = Caffeine.newBuilder()
                .maximumSize(policyConfig.maximumSize())
                .expireAfterWrite(policyConfig.ttl())
                .build();
    }

    @Override
    public void registerRequest(RequestInfo request) {
        cache.put(request.id(), Optional.of(LockInfo.newRequest(request.id(), Instant.now())));
    }

    @Override
    public void timedOut(RequestInfo request, long acquisitionTimeMillis) {
        updateCacheWithNextLockInfo(request.id(), LockState.TIMED_OUT_ACQUIRING);
    }

    @Override
    public void successfulAcquisition(RequestInfo request, long acquisitionTimeMillis) {
        updateCacheWithNextLockInfo(request.id(), LockState.ACQUIRED);
    }

    @Override
    public void lockExpired(UUID requestId, Collection<LockDescriptor> lockDescriptors) {
        updateCacheWithNextLockInfo(requestId, LockState.EXPIRED);
    }

    @Override
    public void explicitlyUnlocked(UUID requestId) {
        updateCacheWithNextLockInfo(requestId, LockState.RELEASED);
    }

    LockDiagnosticInfo getAndLogCurrentState(Set<UUID> requestIds) {
        Map<UUID, Optional<LockInfo>> cacheSnapshot = ImmutableMap.copyOf(cache.asMap());
        Map<UUID, Optional<LockInfo>> viewForRequestIds =
                Maps.toMap(requestIds, requestId -> cacheSnapshot.getOrDefault(requestId, Optional.empty()));
        LockDiagnosticInfo diagnosticInfo = computeLockDiagnosticInfo(viewForRequestIds);

        log.info(
                "Got a request to log lock diagnostic information",
                UnsafeArg.of("info", diagnosticInfo),
                SafeArg.of("requestIds", requestIds));
        return diagnosticInfo;
    }

    void logCurrentState() {
        LockDiagnosticInfo lockDiagnosticInfo = computeLockDiagnosticInfo(ImmutableMap.copyOf(cache.asMap()));
        log.info("Got a request to log lock diagnostic information", UnsafeArg.of("info", lockDiagnosticInfo));
    }

    private static LockDiagnosticInfo computeLockDiagnosticInfo(Map<UUID, Optional<LockInfo>> snapshot) {
        Map<UUID, LockInfo> lockInfos = KeyedStream.stream(snapshot)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collectToMap();

        Set<UUID> requestIdsEvictedMidLockRequest = KeyedStream.stream(snapshot)
                .filter(lockInfo -> !lockInfo.isPresent())
                .keys()
                .collect(Collectors.toSet());

        return ImmutableLockDiagnosticInfo.builder()
                .lockInfos(lockInfos)
                .requestIdsEvictedMidLockRequest(requestIdsEvictedMidLockRequest)
                .build();
    }

    private void updateCacheWithNextLockInfo(UUID requestId, LockState nextLockStage) {
        cache.asMap().compute(requestId, (_requestId, maybeInfo) -> nextLockInfo(maybeInfo, nextLockStage));
    }

    private static Optional<LockInfo> nextLockInfo(Optional<LockInfo> maybeInfo, LockState nextStage) {
        return Optional.ofNullable(maybeInfo)
                .flatMap(Function.identity())
                .map(info -> info.nextStage(nextStage, Instant.now()));
    }
}

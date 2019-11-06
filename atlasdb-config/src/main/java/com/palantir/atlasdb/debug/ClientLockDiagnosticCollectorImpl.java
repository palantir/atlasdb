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

package com.palantir.atlasdb.debug;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.LongStream;

import javax.annotation.Nullable;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.palantir.lock.LockDescriptor;

/**
 * TODO(fdesouza): Remove this once PDS-95791 is resolved
 */
@Deprecated
public class ClientLockDiagnosticCollectorImpl implements ClientLockDiagnosticCollector {

    private final Cache<Long, ClientLockDiagnosticDigest> cache;

    public ClientLockDiagnosticCollectorImpl(LockDiagnosticConfig config) {
        this.cache = Caffeine.newBuilder()
                .maximumSize(config.maximumSize())
                .expireAfterWrite(config.ttl())
                .build();
    }

    @Override
    public void collect(LongStream startTimestamps, long immutableTimestamp, UUID requestId) {
        ClientLockDiagnosticDigest newTransactionDigest =
                ClientLockDiagnosticDigest.newTransaction(immutableTimestamp, requestId);
        startTimestamps.forEach(startTimestamp -> cache.put(startTimestamp, newTransactionDigest));
    }

    @Override
    public void collect(long startTimestamp, UUID requestId, Set<LockDescriptor> lockDescriptors) {
        cache.asMap().compute(
                startTimestamp,
                (_unused, digest) -> getUsableDigest(digest).withLocks(requestId, lockDescriptors));
    }

    private static ClientLockDiagnosticDigest getUsableDigest(@Nullable ClientLockDiagnosticDigest maybeDigest) {
        return MoreObjects.firstNonNull(maybeDigest, ClientLockDiagnosticDigest.newFragment());
    }

    @Override
    public Map<Long, ClientLockDiagnosticDigest> getSnapshot() {
        return ImmutableMap.copyOf(cache.asMap());
    }
}

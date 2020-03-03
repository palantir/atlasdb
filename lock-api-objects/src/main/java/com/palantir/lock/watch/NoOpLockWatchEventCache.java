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

package com.palantir.lock.watch;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableSet;

@SuppressWarnings("FinalClass") // mocks
public class NoOpLockWatchEventCache implements LockWatchEventCache {
    public static final LockWatchEventCache INSTANCE = new NoOpLockWatchEventCache();
    private static final IdentifiedVersion FAKE = ImmutableIdentifiedVersion
            .of(UUID.randomUUID(), Optional.empty());
    private static final TransactionsLockWatchEvents NONE = TransactionsLockWatchEvents.failure(
            LockWatchStateUpdate.snapshot(UUID.randomUUID(), 0L, ImmutableSet.of(), ImmutableSet.of()));

    private NoOpLockWatchEventCache() {
        // singleton
    }

    @Override
    public IdentifiedVersion lastKnownVersion() {
        return FAKE;
    }

    @Override
    public IdentifiedVersion processStartTransactionsUpdate(Set<Long> startTimestamps, LockWatchStateUpdate update) {
        return FAKE;
    }

    @Override
    public void processUpdate(LockWatchStateUpdate update) {
        // noop;
    }

    @Override
    public TransactionsLockWatchEvents getEventsForTransactions(Set<Long> startTimestamps, IdentifiedVersion version) {
        return NONE;
    }
}

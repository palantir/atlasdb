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

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.google.common.collect.ImmutableSet;

public enum NoOpClientLockWatchEventLog implements ClientLockWatchEventLog {
    INSTANCE;

    @Override
    public TransactionsLockWatchEvents getEventsForTransactions(Map<Long, IdentifiedVersion> timestampToVersion,
            IdentifiedVersion version) {
        return TransactionsLockWatchEvents.failure(
                LockWatchStateUpdate.snapshot(UUID.randomUUID(), 0L, ImmutableSet.of(), ImmutableSet.of()));
    }

    @Override
    public IdentifiedVersion getLatestKnownVersion() {
        return IdentifiedVersion.of(UUID.randomUUID(), Optional.empty());
    }

    @Override
    public IdentifiedVersion processUpdate(LockWatchStateUpdate update,
            IdentifiedVersion earliestVersion) {
        return IdentifiedVersion.of(UUID.randomUUID(), Optional.empty());
    }
}

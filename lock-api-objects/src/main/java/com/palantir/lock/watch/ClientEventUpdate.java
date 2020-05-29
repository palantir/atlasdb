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

import java.util.List;
import java.util.Map;

import org.immutables.value.Value;

public interface ClientEventUpdate {
    <T> T accept(Visitor<T> visitor);

    interface Visitor<T> {
        T visit(ClientEvents events);
        T visit(ClientSnapshot snapshot);
    }

    TransactionsLockWatchEvents map(Map<Long, IdentifiedVersion> timestampToVersion);

    static ClientEvents success(List<LockWatchEvent> events) {
        return ImmutableClientEvents.of(events);
    }

    static ClientSnapshot failure(LockWatchStateUpdate.Snapshot snapshot) {
        return ImmutableClientSnapshot.of(snapshot);
    }

    @Value.Immutable
    interface ClientEvents extends ClientEventUpdate {
        @Value.Parameter
        List<LockWatchEvent> events();

        @Override
        default <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }

        @Override
        default TransactionsLockWatchEvents map(Map<Long, IdentifiedVersion> timestampToVersion) {
            return TransactionsLockWatchEvents.success(events(), timestampToVersion);
        }
    }

    @Value.Immutable
    interface ClientSnapshot extends ClientEventUpdate {
        @Value.Parameter
        LockWatchStateUpdate.Snapshot snapshot();

        @Override
        default <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }

        @Override
        default TransactionsLockWatchEvents map(Map<Long, IdentifiedVersion> _timestampToVersion) {
            return TransactionsLockWatchEvents.failure(snapshot());
        }
    }
}

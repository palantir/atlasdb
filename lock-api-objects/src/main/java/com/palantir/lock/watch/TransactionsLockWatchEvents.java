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

/**
 * Represents a condensed view of lock watch events occurring between some known version and a set of start transaction
 * calls.
 */
public interface TransactionsLockWatchEvents {
    <T> T accept(Visitor<T> visitor);

    interface Visitor<T> {
        T visit(Events success);
        T visit(ForcedSnapshot failure);
    }

    static Events success(List<LockWatchEvent> events, Map<Long, IdentifiedVersion> startTsToSequence) {
        return ImmutableEvents.of(events, startTsToSequence);
    }

    static ForcedSnapshot failure(LockWatchStateUpdate.Snapshot snapshot) {
        return ImmutableForcedSnapshot.of(snapshot);
    }

    /**
     * A successful result contains a list of all lock watch events occurring between the last known version and the
     * last started transaction, and a mapping of start timestamps to their respective last occurred event.
     */
    @Value.Immutable
    interface Events extends TransactionsLockWatchEvents {
        @Value.Parameter
        List<LockWatchEvent> events();
        @Value.Parameter
        Map<Long, IdentifiedVersion> startTsToSequence();

        @Override
        default <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    /**
     * A failure denotes that it was not possible to compute the result for all of the requested transactions. Since
     * that generally implies we will fail to get the necessary information for the commit timestamp anyway, instead of
     * giving partial information, we return a single snapshot that should be used to reseed the state of lock watches
     * for all future transactions.
     */
    @Value.Immutable
    interface ForcedSnapshot extends TransactionsLockWatchEvents {
        @Value.Parameter
        LockWatchStateUpdate.Snapshot snapshot();

        @Override
        default <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }
}

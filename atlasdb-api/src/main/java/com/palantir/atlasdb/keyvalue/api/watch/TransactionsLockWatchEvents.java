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

package com.palantir.atlasdb.keyvalue.api.watch;

import java.util.List;
import java.util.Map;

import org.immutables.value.Value;

import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchStateUpdate;

public interface TransactionsLockWatchEvents {
    <T> T accept(Visitor<T> visitor);

    interface Visitor<T> {
        T visit(Success success);
        T visit(Failure failure);
    }

    static Success success(List<LockWatchEvent> events, Map<Long, Long> startTsToSequence) {
        return ImmutableSuccess.of(events, startTsToSequence);
    }

    static Failure failure(LockWatchStateUpdate.Snapshot snapshot) {
        return ImmutableFailure.of(snapshot);
    }

    @Value.Immutable
    interface Success extends TransactionsLockWatchEvents {
        @Value.Parameter
        List<LockWatchEvent> events();
        @Value.Parameter
        Map<Long, Long> startTsToSequence();

        @Override
        default <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    @Value.Immutable
    interface Failure extends TransactionsLockWatchEvents {
        @Value.Parameter
        LockWatchStateUpdate.Snapshot snapshot();

        @Override
        default <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }
}

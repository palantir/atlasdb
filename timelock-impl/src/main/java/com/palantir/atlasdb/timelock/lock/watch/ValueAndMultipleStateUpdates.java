/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.lock.watch;

import com.palantir.lock.watch.LockWatchStateUpdate;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * Return type for operations on a {@link LockWatchingService} that may feature multiple starting versions.
 * At least one of {@link #snapshot()} and {@link #oldestSuccess()} should be present.
 */
@Value.Immutable
public interface ValueAndMultipleStateUpdates<T> {
    @Value.Parameter
    Optional<LockWatchStateUpdate> snapshot();

    @Value.Parameter
    Optional<LockWatchStateUpdate> oldestSuccess();

    @Value.Parameter
    T value();

    static <R> ValueAndMultipleStateUpdates<R> of(
            Optional<LockWatchStateUpdate> maybeSnapshot, Optional<LockWatchStateUpdate> maybeOldestSuccess, R result) {
        return ImmutableValueAndMultipleStateUpdates.of(maybeSnapshot, maybeOldestSuccess, result);
    }

    static <R> ImmutableValueAndMultipleStateUpdates.Builder<R> builder() {
        return ImmutableValueAndMultipleStateUpdates.builder();
    }
}

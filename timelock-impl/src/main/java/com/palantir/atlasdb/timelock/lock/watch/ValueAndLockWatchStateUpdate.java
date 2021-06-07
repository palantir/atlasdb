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

package com.palantir.atlasdb.timelock.lock.watch;

import com.palantir.common.annotations.ImmutablesStyles.PackageVisibleImmutablesStyle;
import com.palantir.lock.watch.LockWatchStateUpdate;
import org.immutables.value.Value;

@Value.Immutable
@PackageVisibleImmutablesStyle
public interface ValueAndLockWatchStateUpdate<T> {
    @Value.Parameter
    LockWatchStateUpdate lockWatchStateUpdate();

    @Value.Parameter
    T value();

    static <R> ValueAndLockWatchStateUpdate<R> of(LockWatchStateUpdate lockWatchStateUpdate, R result) {
        return ImmutableValueAndLockWatchStateUpdate.of(lockWatchStateUpdate, result);
    }
}

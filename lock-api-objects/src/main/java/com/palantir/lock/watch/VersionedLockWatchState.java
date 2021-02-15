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

import com.palantir.common.annotation.Idempotent;
import com.palantir.lock.LockDescriptor;
import java.util.OptionalLong;
import java.util.UUID;

public interface VersionedLockWatchState {
    /**
     * The last known log version that was used to build this lock watch state.
     */
    OptionalLong version();
    /**
     * The id of the timelock leader that the lock watch state is associated with.
     */
    UUID leaderId();
    /**
     * Given a lockDescriptor, returns the associated lock watch info given the current lock watch state.
     */
    @Idempotent
    LockWatchInfo lockWatchState(LockDescriptor lockDescriptor);
    /**
     * Returns the last update that was used to create this state.
     */
    LockWatchStateUpdate lastUpdate();
}

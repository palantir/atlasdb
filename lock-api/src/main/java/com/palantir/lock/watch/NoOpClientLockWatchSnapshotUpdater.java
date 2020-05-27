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

import com.google.common.collect.ImmutableSet;

public enum NoOpClientLockWatchSnapshotUpdater implements ClientLockWatchSnapshotUpdater {
    INSTANCE;

    @Override
    public LockWatchStateUpdate.Snapshot getSnapshot(IdentifiedVersion identifiedVersion) {
        return LockWatchStateUpdate.snapshot(
                identifiedVersion.id(),
                identifiedVersion.version(),
                ImmutableSet.of(),
                ImmutableSet.of());
    }

    @Override
    public void processEvents(List<LockWatchEvent> events) {
    }

    @Override
    public void resetWithSnapshot(LockWatchStateUpdate.Snapshot snapshot) {
    }

    @Override
    public void reset() {
    }
}

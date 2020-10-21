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

package com.palantir.atlasdb.timelock.lock;

import com.palantir.lock.LockDescriptor;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public class CombinedLockEvents implements LockEvents {

    private final List<LockEvents> lockEvents;

    CombinedLockEvents(List<LockEvents> lockEvents) {
        this.lockEvents = lockEvents;
    }

    @Override
    public void registerRequest(RequestInfo request) {
        lockEvents.forEach(events -> events.registerRequest(request));
    }

    @Override
    public void timedOut(RequestInfo request, long acquisitionTimeMillis) {
        lockEvents.forEach(events -> events.timedOut(request, acquisitionTimeMillis));
    }

    @Override
    public void successfulAcquisition(RequestInfo request, long acquisitionTimeMillis) {
        lockEvents.forEach(events -> events.successfulAcquisition(request, acquisitionTimeMillis));
    }

    @Override
    public void lockExpired(UUID requestId, Collection<LockDescriptor> lockDescriptors) {
        lockEvents.forEach(events -> events.lockExpired(requestId, lockDescriptors));
    }

    @Override
    public void explicitlyUnlocked(UUID requestId) {
        lockEvents.forEach(events -> events.explicitlyUnlocked(requestId));
    }
}

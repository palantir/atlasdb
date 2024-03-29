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

package com.palantir.lock.client;

import com.palantir.atlasdb.timelock.api.ConjureLockTokenV2;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequestV2;
import java.util.Set;

public class LegacyLockTokenUnlocker implements LockTokenUnlocker {
    private final NamespacedConjureTimelockService namespacedConjureTimelockService;

    public LegacyLockTokenUnlocker(NamespacedConjureTimelockService namespacedConjureTimelockService) {
        this.namespacedConjureTimelockService = namespacedConjureTimelockService;
    }

    @Override
    public Set<ConjureLockTokenV2> unlock(Set<ConjureLockTokenV2> tokens) {
        return namespacedConjureTimelockService
                .unlockV2(ConjureUnlockRequestV2.of(tokens))
                .get();
    }

    @Override
    public void close() {
        // no op: nothing to close
    }
}

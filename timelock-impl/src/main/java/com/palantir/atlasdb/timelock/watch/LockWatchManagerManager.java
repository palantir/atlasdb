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

package com.palantir.atlasdb.timelock.watch;

import java.util.Set;
import java.util.stream.Collectors;

public class LockWatchManagerManager implements LockWatchManager {
    private final Set<LockWatchManager> managers;
    private final LockEventProcessor aggregatingEventProcessor;

    public LockWatchManagerManager(Set<LockWatchManager> managers) {
        this.managers = managers;
        this.aggregatingEventProcessor = new ForkingLockEventProcessor(managers.stream()
                .map(LockWatchManager::getEventProcessor)
                .collect(Collectors.toSet()));
    }

    @Override
    public LockEventProcessor getEventProcessor() {
        return aggregatingEventProcessor;
    }

    @Override
    public void seedProcessor(LockPredicate predicate, LockWatch watch) {
        managers.forEach(manager -> manager.seedProcessor(predicate, watch));
    }

    @Override
    public void unseedProcessor(LockPredicate predicate, LockWatch watch) {
        managers.forEach(manager -> manager.unseedProcessor(predicate, watch));
    }
}

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

package com.palantir.lock.watch;

import java.util.HashSet;
import java.util.Set;

import com.palantir.lock.LockDescriptor;

public class NewLocksVisitor implements LockWatchEvent.Visitor {
    private final Set<LockDescriptor> locks = new HashSet<>();

    public Set<LockDescriptor> getLocks() {
        return locks;
    }

    @Override
    public void visit(LockEvent lockEvent) {
        locks.addAll(lockEvent.lockDescriptors());
    }

    @Override
    public void visit(UnlockEvent unlockEvent) {
        // noop
    }

    @Override
    public void visit(LockWatchOpenLocksEvent openLocksEvent) {
        // noop
    }

    @Override
    public void visit(LockWatchCreatedEvent lockWatchCreatedEvent) {
        // noop
    }
}

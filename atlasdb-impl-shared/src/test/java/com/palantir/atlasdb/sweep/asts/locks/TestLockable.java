/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.asts.locks;

import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import org.immutables.value.Value;

@Value.Immutable
interface TestLockable extends Comparable<TestLockable> {
    @Value.Parameter
    int value();

    static TestLockable of(int value) {
        return ImmutableTestLockable.of(value);
    }

    @Override
    default int compareTo(TestLockable o) {
        return Integer.compare(value(), o.value());
    }

    default LockDescriptor lockDescriptor() {
        return StringLockDescriptor.of(String.valueOf(value()));
    }
}

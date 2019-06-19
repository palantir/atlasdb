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

import com.palantir.lock.LockDescriptor;

public interface LockEventProcessor {
    static LockEventProcessor createNoOpForTests() {
        return new LockEventProcessor() {
            @Override
            public void registerLock(LockDescriptor descriptor) {
                // intentionally no-op
            }

            @Override
            public void registerUnlock(LockDescriptor descriptor) {
                // intentionally no-op
            }
        };
    }

    void registerLock(LockDescriptor descriptor);

    void registerUnlock(LockDescriptor descriptor);
}

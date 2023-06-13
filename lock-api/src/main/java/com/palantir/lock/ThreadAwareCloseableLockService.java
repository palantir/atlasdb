/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock;

public interface ThreadAwareCloseableLockService extends CloseableLockService {

    /**
     * This is a debugging-only method providing a good-effort guess about which client-thread currently
     * holds a given lock. This information may not reflect the actual state of the world.
     * Returns null if we have not recorded a mapping for this lock.
     * Returns {@link ThreadAwareLockClient#UNKNOWN} if we do not know if this lock is being held or who the current
     * holder thread is.
     */
    ThreadAwareLockClient getHoldingThread(LockDescriptor lock);
}

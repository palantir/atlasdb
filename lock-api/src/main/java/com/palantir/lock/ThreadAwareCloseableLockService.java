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
     * This is a debugging-only method providing best-effort information about which client-thread last successfully
     * acquired a lock. This does not guarantee that the lock is still held by that thread as unlock operations are
     * not recorded.
     * Returns null if we have not recorded a thread acquisition for this lock yet.
     */
    ThreadAwareLockClient getLastAcquiringThread(LockDescriptor lock);
}

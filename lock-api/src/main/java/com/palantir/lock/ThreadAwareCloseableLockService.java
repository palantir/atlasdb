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

import java.util.Map;

public interface ThreadAwareCloseableLockService extends CloseableLockService {

    /**
     * This is a debugging-only method providing a good-effort guess for which lock is currently held
     * by which client thread.
     * This guess is likely based on significantly out-of-date information (as old as a few seconds).
     * Lock descriptors without a mapping (or mapped to null) are assumed to be free at the moment.
     * The {@link ThreadAwareLockClient#UNKNOWN} sentinel value signals that we cannot provide any information about
     * which thread currently owns the lock.
     */
    Map<LockDescriptor, ThreadAwareLockClient> getLastKnownThreadInfoSnapshot();
}

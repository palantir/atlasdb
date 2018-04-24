/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.lock.impl;

import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;

/**
 * A reentrant read-write lock which uses explicit {@link LockClient} tokens,
 * not threads, to identify the holders of locks.
 *
 * @author jtamer
 */
public interface ClientAwareReadWriteLock {

    /** Returns a {@link LockDescriptor} describing this lock. */
    LockDescriptor getDescriptor();

    /** Returns the specified type of lock for the given lock client. */
    KnownClientLock get(LockClient client, LockMode mode);

    /** Returns {@code true} iff the lock is in a frozen state. */
    boolean isFrozen();
}

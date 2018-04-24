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
package com.palantir.lock;

import java.util.concurrent.locks.ReadWriteLock;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * A descriptor for a {@link ReadWriteLock}, identified by a lock ID (a unique
 * string).
 *
 * @author jtamer
 */
public final class StringLockDescriptor {

    private StringLockDescriptor() {
        // cannot instantiate
    }

    /** Returns a {@code LockDescriptor} instance for the given lock ID. */
    @SuppressWarnings("checkstyle:jdkStandardCharsets") // StandardCharsets only in JDK 1.7+
    public static LockDescriptor of(String lockId) {
        Preconditions.checkNotNull(lockId, "lockId should not be null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(lockId));
        return new LockDescriptor(lockId.getBytes(Charsets.UTF_8));
    }
}

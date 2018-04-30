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

import com.google.common.primitives.Longs;

/**
 * A descriptor for a {@link ReadWriteLock}, identified by a lock ID (a unique
 * string).
 *
 * @author jtamer
 */
public final class AtlasTimestampLockDescriptor {

    private AtlasTimestampLockDescriptor() {
        // cannot instantiate
    }

    /** Returns a {@code LockDescriptor} instance for the given table and row. */
    public static LockDescriptor of(long timestamp) {
        return new LockDescriptor(Longs.toByteArray(timestamp));
    }
}

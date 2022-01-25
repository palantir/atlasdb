/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.common.concurrent;

import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.lang.reflect.Field;
import java.security.PrivilegedAction;

@SuppressWarnings({"UnnecessarilyQualified", "deprecation", "removal"}) // Internal classes should not be imported
public final class ThreadNames {

    private static final sun.misc.Unsafe unsafe = initUnsafe();

    private static final long threadNameOffset;

    static {
        try {
            threadNameOffset = unsafe.objectFieldOffset(Thread.class.getDeclaredField("name"));
        } catch (NoSuchFieldException e) {
            throw new SafeIllegalStateException("Failed to find the Thread.name field", e);
        }
    }

    /**
     * Sets the name of a thread without native overhead to rename the OS thread.
     * Note that unlike {@link Thread#setName(String)} this implementation may not
     * synchronize on the {@code thread} object.
     *
     * @see <a href="https://github.com/palantir/atlasdb/pull/5796>atlasdb#5796</a>
     */
    public static void setThreadName(Thread thread, String name) {
        Preconditions.checkNotNull(thread, "Thread is required");
        Preconditions.checkNotNull(name, "Thread name is required");
        unsafe.putObjectVolatile(thread, threadNameOffset, name);
    }

    // prevent avoidable failures in spark/etc where security manager is used
    @SuppressWarnings("removal")
    private static sun.misc.Unsafe initUnsafe() {
        return java.security.AccessController.doPrivileged((PrivilegedAction<sun.misc.Unsafe>) () -> {
            try {
                Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
                field.setAccessible(true);
                return (sun.misc.Unsafe) field.get(null);
            } catch (ReflectiveOperationException e) {
                throw new SafeIllegalStateException("Failed to load Unsafe", e);
            }
        });
    }

    private ThreadNames() {}
}

// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.lock;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.concurrent.locks.ReadWriteLock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.primitives.Longs;

/**
 * A descriptor for a {@link ReadWriteLock}, identified by a lock ID (a unique
 * string).
 *
 * @author jtamer
 */
@Immutable
public final class AtlasTimestampLockDescriptor extends LockDescriptor {
    private static final long serialVersionUID = 3L;

    /** Returns a {@code LockDescriptor} instance for the given table and row. */
    public static AtlasTimestampLockDescriptor of(long timestamp) {
        return new AtlasTimestampLockDescriptor(Longs.toByteArray(timestamp));
    }

    private AtlasTimestampLockDescriptor(byte[] bytes) {
        super(bytes);
    }

    @Override
    public String getLockId() {
        return "AtlasDb immutable ts lock: " + Longs.fromByteArray(bytes);
    }

    private void readObject(@SuppressWarnings("unused") ObjectInputStream in)
            throws InvalidObjectException {
        throw new InvalidObjectException("proxy required");
    }

    private Object writeReplace() {
        return new SerializationProxy(this);
    }

    private static class SerializationProxy implements Serializable {
        private static final long serialVersionUID = 4L;

        @Nullable
        private final byte[] bytes;

        SerializationProxy(AtlasTimestampLockDescriptor lockDescriptor) {
            bytes = lockDescriptor.bytes;
        }

        Object readResolve() {
            return new AtlasTimestampLockDescriptor(bytes);
        }
    }
}

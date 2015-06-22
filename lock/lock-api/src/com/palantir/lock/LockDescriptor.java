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

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.locks.ReadWriteLock;

import com.google.common.primitives.UnsignedBytes;

/**
 * A descriptor for a {@link ReadWriteLock}.
 *
 * @author jtamer
 */
public abstract class LockDescriptor implements Comparable<LockDescriptor>, Serializable {
    private static final long serialVersionUID = 1L;
    protected final byte[] bytes;

    protected LockDescriptor(byte[] bytes) {
        this.bytes = bytes;
    }

    /** Returns the ID of the read-write lock identified by this descriptor. */
    public abstract String getLockId();

    @Override
    public int compareTo(LockDescriptor o) {
        return UnsignedBytes.lexicographicalComparator().compare(this.bytes, o.bytes);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " [" + getLockId() +"]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(bytes);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        LockDescriptor other = (LockDescriptor) obj;
        if (!Arrays.equals(bytes, other.bytes))
            return false;
        return true;
    }
}

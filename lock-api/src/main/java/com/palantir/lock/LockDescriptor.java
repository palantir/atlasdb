/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.locks.ReadWriteLock;

import javax.annotation.concurrent.Immutable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.CharMatcher;
import com.google.common.base.Charsets;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.UnsignedBytes;

/**
 * A descriptor for a {@link ReadWriteLock}.
 *
 * @author jtamer
 */
@Immutable
public class LockDescriptor implements Comparable<LockDescriptor>, Serializable {

    private static final long serialVersionUID = 1L;
    private static final CharMatcher BASIC_PRINTABLE_ASCII = CharMatcher.inRange(' ', '~');

    private final byte[] bytes;

    @JsonCreator
    LockDescriptor(@JsonProperty("bytes") byte[] bytes) {
        this.bytes = bytes;
    }

    @JsonIgnore
    public String getLockIdAsString() {
        return new String(bytes, Charsets.UTF_8);
    }

    @Override
    public int compareTo(LockDescriptor o) {
        return UnsignedBytes.lexicographicalComparator().compare(this.bytes, o.bytes);
    }

    @Override
    public String toString() {
        String lockIdAsString = getLockIdAsString();
        return getClass().getSimpleName() + " [" +
                (BASIC_PRINTABLE_ASCII.matchesAllOf(lockIdAsString) ?
                        lockIdAsString :
                        BaseEncoding.base16().encode(bytes))
                + "]";
    }

    public byte[] getBytes() {
        return bytes.clone();
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

// Copyright 2015 Palantir Technologies
//
// Licensed under the BSD-3 License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://opensource.org/licenses/BSD-3-Clause
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.keyvalue.api;

import java.io.Serializable;
import java.util.Arrays;

import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.io.BaseEncoding;

/**
 * Represents a value in the key-value store (including its timestamp).
 * Values are stored in {@link Cell}s.
 * @see Cell
 */
public class Value implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final long INVALID_VALUE_TIMESTAMP = -1L;
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    public static Value create(byte[] contents, long timestamp) {
        if (contents == null) {
            contents = EMPTY_BYTE_ARRAY;
        }
        return new Value(contents, timestamp);
    }

    /**
     * The contents of the value.
     */
    @Nonnull
    public byte[] getContents() {
        return contents;
    }

    /**
     * The timestamp of the value.
     */
    public long getTimestamp() {
        return timestamp;
    }

    private final byte[] contents;
    private final long timestamp;

    private Value(byte[] contents, long timestamp) {
        Preconditions.checkArgument((timestamp >= 0 && timestamp < Long.MAX_VALUE) || (timestamp == INVALID_VALUE_TIMESTAMP), "timestamp out of bounds");
        this.contents = contents;
        this.timestamp = timestamp;
    }

    public static final Function<Value, Long> GET_TIMESTAMP = new Function<Value, Long>() {
        @Override
        public Long apply(Value from) {
            return from.getTimestamp();
        }
    };

    public static final Function<Value, byte[]> GET_VALUE = new Function<Value, byte[]>() {
        @Override
        public byte[] apply(Value from) {
            return from.getContents();
        }
    };

    public static final Predicate<byte[]> IS_EMPTY = new Predicate<byte[]>() {
        @Override
        public boolean apply(byte[] input) {
            return input.length == 0;
        }
    };

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(contents);
        result = prime * result + (int)(timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Value other = (Value)obj;
        if (!Arrays.equals(contents, other.contents)) {
            return false;
        }
        if (timestamp != other.timestamp) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        byte[] prefix = Arrays.copyOf(contents, Math.min(10, contents.length));
        return "Value [contents=" + new String(BaseEncoding.base16().lowerCase().encode(prefix)) + ", timestamp=" + timestamp + "]";
    }


}

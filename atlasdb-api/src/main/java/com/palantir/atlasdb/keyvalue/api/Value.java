/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.logsafe.Preconditions;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Represents a value in the key-value store (including its timestamp).
 * Values are stored in {@link Cell}s.
 * @see Cell
 */
public final class Value implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final long INVALID_VALUE_TIMESTAMP = -1L;

    @JsonCreator
    public static Value create(@JsonProperty("contents") byte[] contents, @JsonProperty("timestamp") long timestamp) {
        return new Value(MoreObjects.firstNonNull(contents, PtBytes.EMPTY_BYTE_ARRAY), timestamp);
    }

    public static Value createWithCopyOfData(byte[] contents, long timestamp) {
        return Value.create(Arrays.copyOf(contents, contents.length), timestamp);
    }

    /**
     * The contents of the value.
     */
    @Nonnull
    public byte[] getContents() {
        return contents;
    }

    public boolean isEmpty() {
        return contents.length == 0;
    }

    public static boolean isTombstone(byte[] value) {
        return value.length == 0;
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
        Preconditions.checkArgument(
                (timestamp >= 0 && timestamp < Long.MAX_VALUE) || (timestamp == INVALID_VALUE_TIMESTAMP),
                "timestamp out of bounds");
        this.contents = contents;
        this.timestamp = timestamp;
    }

    public static final Function<Value, Long> GET_TIMESTAMP = Value::getTimestamp;

    public static final Function<Value, byte[]> GET_VALUE = Value::getContents;

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Value value = (Value) obj;
        return timestamp == value.timestamp && Arrays.equals(contents, value.contents);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(contents), timestamp);
    }

    @Override
    public String toString() {
        byte[] prefix = Arrays.copyOf(contents, Math.min(10, contents.length));
        return "Value [contents=" + PtBytes.encodeHexString(prefix)
                + ", contentsLength=" + contents.length
                + ", timestamp=" + timestamp + "]";
    }
}

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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.encoding.PtBytes;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class CheckAndSetException extends AtomicWriteException {
    private static final long serialVersionUID = 1L;

    private final Cell key;

    public CheckAndSetException(Cell cell, String msg, Throwable ex) {
        this(msg, ex, cell, PtBytes.EMPTY_BYTE_ARRAY, ImmutableList.of());
    }

    @VisibleForTesting
    public CheckAndSetException(String msg) {
        this(
                msg,
                Cell.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY),
                PtBytes.EMPTY_BYTE_ARRAY,
                ImmutableList.of());
    }

    public CheckAndSetException(String msg, Throwable ex, Cell key, byte[] expected, List<byte[]> actual) {
        super(
                msg,
                ex,
                Collections.singletonMap(key, expected),
                Collections.singletonMap(key, Iterables.getOnlyElement(actual, null)));
        this.key = key;
    }

    public CheckAndSetException(String msg, Cell key, byte[] expected, List<byte[]> actual) {
        super(
                msg,
                Collections.singletonMap(key, expected),
                Collections.singletonMap(key, Iterables.getOnlyElement(actual, null)));
        this.key = key;
    }

    public CheckAndSetException(Cell key, TableReference table, byte[] expected, List<byte[]> actual) {
        this(
                String.format(
                        "Unexpected value observed in table %s. "
                                + "If this is happening repeatedly, your program may be out of sync with the database.",
                        table),
                key,
                expected,
                actual);
    }

    public Cell getKey() {
        return key;
    }

    public byte[] getExpectedValue() {
        return getExpectedValues().get(key);
    }

    public List<byte[]> getActualValues() {
        return Optional.ofNullable(getObservedValues().get(key))
                .map(ImmutableList::of)
                .orElseGet(ImmutableList::of);
    }
}

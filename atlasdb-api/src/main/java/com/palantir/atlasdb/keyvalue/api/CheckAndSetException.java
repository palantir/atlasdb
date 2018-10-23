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

import java.util.List;

public class CheckAndSetException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final Cell key;
    private final byte[] expectedValue;
    private final List<byte[]> actualValues;

    public CheckAndSetException(String msg, Throwable ex) {
        this(msg, ex, null, null, null);
    }

    public CheckAndSetException(String msg) {
        this(msg, null, null, null);
    }

    public CheckAndSetException(String msg, Throwable ex, Cell key, byte[] expectedValue, List<byte[]> actualValues) {
        super(msg, ex);
        this.key = key;
        this.expectedValue = expectedValue;
        this.actualValues = actualValues;
    }

    public CheckAndSetException(String msg, Cell key, byte[] expectedValue, List<byte[]> actualValues) {
        super(msg);
        this.key = key;
        this.expectedValue = expectedValue;
        this.actualValues = actualValues;
    }

    public CheckAndSetException(Cell key, TableReference table, byte[] expected, List<byte[]> actual) {
        super(String.format(
                "Unexpected value observed in table %s. "
                        + "If this is happening repeatedly, your program may be out of sync with the database.",
                table));
        this.key = key;
        this.expectedValue = expected;
        this.actualValues = actual;
    }

    public Cell getKey() {
        return key;
    }

    public byte[] getExpectedValue() {
        return expectedValue;
    }

    public List<byte[]> getActualValues() {
        return actualValues;
    }
}

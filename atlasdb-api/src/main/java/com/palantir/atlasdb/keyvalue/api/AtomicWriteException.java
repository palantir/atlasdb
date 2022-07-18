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

package com.palantir.atlasdb.keyvalue.api;

import java.util.Map;

public class AtomicWriteException extends RuntimeException {
    private final Map<Cell, byte[]> expectedValues;
    private final Map<Cell, byte[]> observedValues;

    public AtomicWriteException(String msg, Map<Cell, byte[]> expectedValue, Map<Cell, byte[]> observedValues) {
        super(msg);
        this.expectedValues = expectedValue;
        this.observedValues = observedValues;
    }

    public AtomicWriteException(
            String msg, Throwable ex, Map<Cell, byte[]> expectedValue, Map<Cell, byte[]> observedValues) {
        super(msg, ex);
        this.expectedValues = expectedValue;
        this.observedValues = observedValues;
    }

    public Map<Cell, byte[]> getExpectedValues() {
        return expectedValues;
    }

    public Map<Cell, byte[]> getObservedValues() {
        return observedValues;
    }
}

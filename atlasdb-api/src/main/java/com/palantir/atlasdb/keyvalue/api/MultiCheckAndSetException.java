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

import com.google.errorprone.annotations.CompileTimeConstant;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.util.Map;

public class MultiCheckAndSetException extends RuntimeException {
    private final byte[] rowName;
    private final Map<Cell, byte[]> expectedValues;
    private final Map<Cell, byte[]> actualValues;

    public MultiCheckAndSetException(@CompileTimeConstant String message) {
        super(new SafeRuntimeException(message));
        this.rowName = null;
        this.expectedValues = null;
        this.actualValues = null;
    }

    public MultiCheckAndSetException(
            Arg<String> tableReference,
            byte[] rowName,
            Map<Cell, byte[]> expectedValue,
            Map<Cell, byte[]> actualValues) {
        super(new SafeRuntimeException(
                "Current values in the database do not match the expected values specified in multi-checkAndSet"
                        + " request.",
                tableReference));
        this.rowName = rowName;
        this.expectedValues = expectedValue;
        this.actualValues = actualValues;
    }

    public byte[] getRowName() {
        return rowName;
    }

    public Map<Cell, byte[]> getExpectedValues() {
        return expectedValues;
    }

    public Map<Cell, byte[]> getActualValues() {
        return actualValues;
    }
}

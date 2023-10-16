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

import com.google.common.collect.ImmutableList;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeLoggable;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeExceptions;
import java.util.List;
import java.util.Map;

public class MultiCheckAndSetException extends RuntimeException implements SafeLoggable {
    private static final String MESSAGE =
            "Current values in the database do not match the expected values specified in multi-checkAndSet"
                    + " request.";
    private final byte[] rowName;
    private final Map<Cell, byte[]> expectedValues;
    private final Map<Cell, byte[]> actualValues;

    private final List<Arg<?>> args;

    public MultiCheckAndSetException(
            Arg<String> tableReference,
            byte[] rowName,
            Map<Cell, byte[]> expectedValue,
            Map<Cell, byte[]> actualValues,
            Arg<?>... args) {
        this(
                rowName,
                expectedValue,
                actualValues,
                toArgListWithTableNameRowAndValues(tableReference, rowName, expectedValue, actualValues, args));
    }

    private MultiCheckAndSetException(
            byte[] rowName, Map<Cell, byte[]> expectedValue, Map<Cell, byte[]> actualValues, List<Arg<?>> args) {
        super(SafeExceptions.renderMessage(MESSAGE, args.toArray(new Arg[0])));
        this.rowName = rowName;
        this.expectedValues = expectedValue;
        this.actualValues = actualValues;
        this.args = args;
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

    @Override
    public @Safe String getLogMessage() {
        return MESSAGE;
    }

    @Override
    public List<Arg<?>> getArgs() {
        return args;
    }

    private static List<Arg<?>> toArgListWithTableNameRowAndValues(
            Arg<String> tableReference,
            byte[] rowName,
            Map<Cell, byte[]> expectedValue,
            Map<Cell, byte[]> actualValues,
            Arg<?>... args) {
        return ImmutableList.<Arg<?>>builderWithExpectedSize(args.length + 4)
                .add(args)
                .add(tableReference)
                .add(UnsafeArg.of("rowName", rowName))
                .add(UnsafeArg.of("expectedValue", expectedValue))
                .add(UnsafeArg.of("actualValues", actualValues))
                .build();
    }
}

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

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CompileTimeConstant;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeLoggable;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeExceptions;
import java.util.List;
import javax.annotation.Nullable;

public class CheckAndSetException extends RuntimeException implements SafeLoggable {
    private static final String DEFAULT_MESSAGE = "Unexpected value observed in table."
            + " If this is happening repeatedly, your program may be out of sync with the database.";
    private static final long serialVersionUID = 1L;

    @Nullable
    private final Cell key;

    @Nullable
    private final byte[] expectedValue;

    @Nullable
    private final List<byte[]> actualValues;

    @CompileTimeConstant
    private final String logMessage;

    private final List<Arg<?>> args;

    public CheckAndSetException(@CompileTimeConstant String logMessage, Throwable ex, Arg<?>... args) {
        this(logMessage, ex, null, null, null, toArgListWithKeyAndValues(null, null, null, args));
    }

    public CheckAndSetException(@CompileTimeConstant String logMessage, Arg<?>... args) {
        this(logMessage, null, null, null, null, toArgListWithKeyAndValues(null, null, null, args));
    }

    public CheckAndSetException(
            @CompileTimeConstant String logMessage,
            Throwable ex,
            Cell key,
            byte[] expectedValue,
            List<byte[]> actualValues,
            Arg<?>... args) {
        this(
                logMessage,
                ex,
                key,
                expectedValue,
                actualValues,
                toArgListWithKeyAndValues(key, expectedValue, actualValues, args));
    }

    public CheckAndSetException(
            @CompileTimeConstant String logMessage,
            Cell key,
            byte[] expectedValue,
            List<byte[]> actualValues,
            Arg<?>... args) {
        this(
                logMessage,
                null,
                key,
                expectedValue,
                actualValues,
                toArgListWithKeyAndValues(key, expectedValue, actualValues, args));
    }

    public CheckAndSetException(Cell key, byte[] expectedValue, List<byte[]> actualValues, Arg<?>... args) {
        this(DEFAULT_MESSAGE, null, key, expectedValue, actualValues, args);
    }

    private CheckAndSetException(
            @CompileTimeConstant String logMessage,
            @Nullable Throwable cause,
            @Nullable Cell key,
            @Nullable byte[] expectedValue,
            @Nullable List<byte[]> actualValues,
            List<Arg<?>> args) {
        super(SafeExceptions.renderMessage(logMessage, args.toArray(new Arg[0])), cause);
        this.logMessage = logMessage;
        this.key = key;
        this.expectedValue = expectedValue;
        this.actualValues = actualValues;
        this.args = args;
    }

    @Nullable
    public Cell getKey() {
        return key;
    }

    @Nullable
    public byte[] getExpectedValue() {
        return expectedValue;
    }

    @Nullable
    public List<byte[]> getActualValues() {
        return actualValues;
    }

    @Override
    public @Safe String getLogMessage() {
        return logMessage;
    }

    @Override
    public List<Arg<?>> getArgs() {
        return args;
    }

    private static List<Arg<?>> toArgListWithKeyAndValues(
            @Nullable Cell key, @Nullable byte[] expectedValue, @Nullable List<byte[]> actualValues, Arg<?>... args) {
        return ImmutableList.<Arg<?>>builderWithExpectedSize(args.length + 3)
                .add(args)
                .add(UnsafeArg.of("key", key))
                .add(UnsafeArg.of("expectedValue", expectedValue))
                .add(UnsafeArg.of("actualValues", actualValues))
                .build();
    }
}

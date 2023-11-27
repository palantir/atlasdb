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

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CompileTimeConstant;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeLoggable;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeExceptions;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A {@link KeyAlreadyExistsException} is thrown if an operation that conditionally updates a {@link KeyValueService}
 * fails because some data is already present in the underlying database.
 */
public class KeyAlreadyExistsException extends RuntimeException implements SafeLoggable {
    private static final long serialVersionUID = 1L;
    private final String logMessage;
    private final List<Arg<?>> args;

    /**
     * The {@link Cell}s present in this list contributed to the failure of the conditional update, in that they were
     * already present when the conditional update expected them to not be present. This list may not be complete;
     * there may be additional cells that the conditional update expected to not be present that are actually present.
     */
    private final ImmutableList<Cell> existingKeys;
    /**
     * Some conditional updates may partially succeed; if so, {@link Cell}s which were known to be successfully
     * committed may be placed in this list. This list may not be complete; there may be additional cells that
     * were actually successfully committed but are not in this list.
     */
    @SuppressWarnings("checkstyle:MutableException") // Not final for backwards compatibility in serialization.
    private ImmutableList<Cell> knownSuccessfullyCommittedKeys;

    public KeyAlreadyExistsException(@CompileTimeConstant String logMessage, Throwable ex, Arg<?>... args) {
        this(logMessage, ex, ImmutableList.of(), ImmutableList.of(), toArgListWithUnsafeCells(args));
    }

    public KeyAlreadyExistsException(@CompileTimeConstant String logMessage, Arg<?>... args) {
        this(logMessage, null, ImmutableList.of(), ImmutableList.of(), toArgListWithUnsafeCells(args));
    }

    public KeyAlreadyExistsException(
            @CompileTimeConstant String logMessage, Throwable ex, Iterable<Cell> existingKeys, Arg<?>... args) {
        this(logMessage, ex, existingKeys, ImmutableList.of(), toArgListWithUnsafeCells(existingKeys, args));
    }

    public KeyAlreadyExistsException(
            @CompileTimeConstant String logMessage, Iterable<Cell> existingKeys, Arg<?>... args) {
        this(logMessage, null, existingKeys, ImmutableList.of(), toArgListWithUnsafeCells(existingKeys, args));
    }

    public KeyAlreadyExistsException(
            @CompileTimeConstant String logMessage,
            Iterable<Cell> existingKeys,
            Iterable<Cell> knownSuccessfullyCommittedKeys,
            Arg<?>... args) {
        this(
                logMessage,
                null,
                existingKeys,
                knownSuccessfullyCommittedKeys,
                toArgListWithUnsafeCells(existingKeys, knownSuccessfullyCommittedKeys, args));
    }

    private KeyAlreadyExistsException(
            @CompileTimeConstant String logMessage,
            @Nullable Throwable cause,
            Iterable<Cell> existingKeys,
            Iterable<Cell> knownSuccessfullyCommittedKeys,
            List<Arg<?>> args) {
        super(SafeExceptions.renderMessage(logMessage, args.toArray(new Arg[0])), cause);
        this.args = args;
        this.logMessage = logMessage;
        this.existingKeys = ImmutableList.copyOf(existingKeys);
        this.knownSuccessfullyCommittedKeys = ImmutableList.copyOf(knownSuccessfullyCommittedKeys);
    }

    public Collection<Cell> getExistingKeys() {
        return existingKeys;
    }

    public Collection<Cell> getKnownSuccessfullyCommittedKeys() {
        return knownSuccessfullyCommittedKeys;
    }

    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        knownSuccessfullyCommittedKeys = MoreObjects.firstNonNull(knownSuccessfullyCommittedKeys, ImmutableList.of());
    }

    @Override
    public @Safe String getLogMessage() {
        return logMessage;
    }

    @Override
    public List<Arg<?>> getArgs() {
        return args;
    }

    private static List<Arg<?>> toArgListWithUnsafeCells(Iterable<Cell> existingKeys, Arg<?>... args) {
        return toArgListWithUnsafeCells(ImmutableList.copyOf(existingKeys), ImmutableList.of(), args);
    }

    private static List<Arg<?>> toArgListWithUnsafeCells(Arg<?>... args) {
        return toArgListWithUnsafeCells(ImmutableList.of(), ImmutableList.of(), args);
    }

    private static List<Arg<?>> toArgListWithUnsafeCells(
            Iterable<Cell> existingKeys, Iterable<Cell> knownSuccessfullyCommittedKeys, Arg<?>... args) {
        return ImmutableList.<Arg<?>>builderWithExpectedSize(args.length + 2)
                .add(args)
                .add(
                        UnsafeArg.of("existingKeys", existingKeys),
                        UnsafeArg.of("knownSuccessfullyCommittedKeys", knownSuccessfullyCommittedKeys))
                .build();
    }
}

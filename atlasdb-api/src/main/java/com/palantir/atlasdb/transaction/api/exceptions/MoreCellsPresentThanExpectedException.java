/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.api.exceptions;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.SafeLoggable;
import com.palantir.logsafe.Unsafe;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeExceptions;
import java.util.List;
import java.util.Map;

public final class MoreCellsPresentThanExpectedException extends IllegalStateException implements SafeLoggable {

    private static final String MESSAGE =
            "KeyValueService returned more results than Get expected. This means there is a bug"
                    + "either in the SnapshotTransaction implementation or in how the client is "
                    + "using such method.";
    private final List<Arg<?>> arguments;
    private final Map<Cell, byte[]> fetchedCells;

    public MoreCellsPresentThanExpectedException(Map<Cell, byte[]> fetchedCells, long expectedNumberOfCells) {
        super(SafeExceptions.renderMessage(
                MESSAGE, argsFrom(fetchedCells, expectedNumberOfCells).toArray(Arg<?>[]::new)));
        this.fetchedCells = fetchedCells;
        this.arguments = argsFrom(fetchedCells, expectedNumberOfCells);
    }

    public Map<Cell, byte[]> getFetchedCells() {
        return fetchedCells;
    }

    @Override
    public @Safe String getLogMessage() {
        return MESSAGE;
    }

    @Override
    public List<Arg<?>> getArgs() {
        return arguments;
    }

    @Unsafe private static List<Arg<?>> argsFrom(Map<Cell, byte[]> retrievedCells, long expectedNumberOfCells) {
        return List.of(
                SafeArg.of("expectedNumberOfCells", expectedNumberOfCells),
                SafeArg.of("numberOfCellsRetrieved", retrievedCells.size()),
                UnsafeArg.of("retrievedCells", retrievedCells));
    }
}

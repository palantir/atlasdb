/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.logging;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.logsafe.Arg;
import java.util.Optional;
import java.util.function.Function;

/**
 * Normally, rows, dynamic columns and values are unsafe for logging; this is because they may contain user data.
 * However, there may be circumstances where publishing information about these may be permissible. For example, if a
 * user is storing information in a queue, the indexes of the queue may be eligible to be considered as safe
 * information (even if the values in the queue itself may not).
 *
 * Methods in this class MAY return Optional.empty() to indicate that they are not able to make a final
 * decision as to what arguments should be produced.
 */
public interface SensitiveLoggingArgProducer {
    Optional<Arg<?>> getArgForRow(TableReference tableReference, byte[] row, Function<byte[], Object> transform);

    Optional<Arg<?>> getArgForColumn(TableReference tableReference, byte[] row, Function<byte[], Object> transform);

    default Optional<Arg<?>> getArgForValue(
            TableReference tableReference, Cell cellReference, byte[] value, Function<byte[], Object> transform) {
        return getNamedArgForValue(tableReference, cellReference, value, transform, "value");
    }

    Optional<Arg<?>> getNamedArgForValue(
            TableReference tableReference,
            Cell cellReference,
            byte[] value,
            Function<byte[], Object> transform,
            String name);
}

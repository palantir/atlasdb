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

package com.palantir.atlasdb.keyvalue.api;

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableException;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.Test;

public class MultiCheckAndSetExceptionTest {
    private static final byte[] ROW_NAME = bytes("row");
    private static final Cell CELL = Cell.create(ROW_NAME, bytes("cell"));
    private static final Cell OTHER_CELL = Cell.create(bytes("other_row"), bytes("other_cell"));
    private static final Map<Cell, byte[]> EXPECTED_VALUES =
            Map.of(CELL, bytes("value"), OTHER_CELL, bytes("other_value"));
    private static final Map<Cell, byte[]> ACTUAL_VALUES =
            Map.of(CELL, bytes("diff_value"), OTHER_CELL, bytes("diff_other_value"));

    @Test
    public void allArgsIncludingTableReferencePresent() {
        Arg<?>[] extraArgs =
                new Arg[] {UnsafeArg.of("test", "test-value"), SafeArg.of("other-test", "other-test-value")};
        assertAllArgsPresent(SafeArg.of("tableReference", "table-reference"), extraArgs);

        assertAllArgsPresent(UnsafeArg.of("tableReference", "table-reference"), extraArgs);
    }

    @Test
    public void getLogMessageReturnsRawMessage() {
        assertThat(multiCheckAndSetException(SafeArg.of("tableReference", "table-reference"))
                        .getLogMessage())
                .isEqualTo("Current values in the database do not match the expected values"
                        + " specified in multi-checkAndSet request.");
    }

    private static void assertAllArgsPresent(Arg<String> tableReference, Arg<?>... extraArgs) {
        // 1 table reference, 3 for row name, expected values and actual values, + whatever is in extra args
        Arg<?>[] expectedArgs = ImmutableList.<Arg<?>>builderWithExpectedSize(1 + 3 + extraArgs.length)
                .add(extraArgs)
                .add(tableReference)
                .add(UnsafeArg.of("rowName", ROW_NAME))
                .add(UnsafeArg.of("expectedValues", EXPECTED_VALUES))
                .add(UnsafeArg.of("actualValues", ACTUAL_VALUES))
                .build()
                .toArray(new Arg[0]);

        assertThatLoggableException(multiCheckAndSetException(tableReference, extraArgs))
                .hasExactlyArgs(expectedArgs);
    }

    private static MultiCheckAndSetException multiCheckAndSetException(
            Arg<String> tableReference, Arg<?>... extraArgs) {
        return new MultiCheckAndSetException(tableReference, ROW_NAME, EXPECTED_VALUES, ACTUAL_VALUES, extraArgs);
    }

    private static byte[] bytes(String string) {
        return string.getBytes(StandardCharsets.UTF_8);
    }
}

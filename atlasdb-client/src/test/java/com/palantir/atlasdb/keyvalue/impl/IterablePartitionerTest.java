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
package com.palantir.atlasdb.keyvalue.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Lists;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.slf4j.Logger;

public class IterablePartitionerTest {
    private static final String PARAMETERIZED_TEST_NAME = "tableName={0}";

    // approx put size is intentionally larger than the max (this triggers logging)
    private static final long LARGE_PUT_SIZE = 20L;
    private static final long MAXIMUM_PUT_SIZE = 10L;
    private static final long SMALL_PUT_SIZE = 6L;

    public static List<String> tableNames() {
        return List.of("test", "foo.bar", "[intentionally.invalid.table.name, foo.bar.baz]");
    }

    @SuppressWarnings("Slf4jConstantLogMessage")
    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("tableNames")
    public void testWithLogging(String tableName) {
        Logger mockLogger = Mockito.mock(Logger.class);
        Mockito.when(mockLogger.isWarnEnabled()).thenReturn(true);

        simplePartition(mockLogger, LARGE_PUT_SIZE, tableName);

        // verify the correct log messages were sent
        Mockito.verify(mockLogger, Mockito.times(3)).isWarnEnabled();
        Mockito.verify(mockLogger, Mockito.times(3))
                .warn(
                        Mockito.anyString(),
                        Mockito.eq(SafeArg.of("approximatePutSize", LARGE_PUT_SIZE)),
                        Mockito.eq(SafeArg.of("maximumPutSize", MAXIMUM_PUT_SIZE)),
                        Mockito.eq(UnsafeArg.of("tableName", tableName)));
        Mockito.verifyNoMoreInteractions(mockLogger);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("tableNames")
    public void testWithoutLogging(String tableName) {
        Logger mockLogger = Mockito.mock(Logger.class);
        Mockito.when(mockLogger.isWarnEnabled()).thenReturn(false);

        simplePartition(mockLogger, LARGE_PUT_SIZE, tableName);

        // warn isn't enabled, so it should check 3 times but not log anything
        Mockito.verify(mockLogger, Mockito.times(3)).isWarnEnabled();
        Mockito.verifyNoMoreInteractions(mockLogger);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("tableNames")
    public void smallPutsDoNotLog(String tableName) {
        Logger mockLogger = Mockito.mock(Logger.class);
        Mockito.when(mockLogger.isWarnEnabled()).thenReturn(true);

        simplePartition(mockLogger, SMALL_PUT_SIZE, tableName);

        // verify the log messages were not sent
        Mockito.verifyNoMoreInteractions(mockLogger);
    }

    private void simplePartition(Logger mockLogger, long approximatePutSize, String tableName) {
        Iterable<List<Integer>> partitions = IterablePartitioner.partitionByCountAndBytes(
                Lists.newArrayList(1, 2, 3), 2, MAXIMUM_PUT_SIZE, tableName, foo -> approximatePutSize, mockLogger);
        int i = 1;
        for (List<Integer> partition : partitions) {
            assertThat(partition).containsExactly(i);
            i++;
        }
    }
}

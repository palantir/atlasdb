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

import com.google.common.collect.Lists;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import java.util.List;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.slf4j.Logger;

@RunWith(Parameterized.class)
public class IterablePartitionerTest {

    // approx put size is intentionally larger than the max (this triggers logging)
    private static final long LARGE_PUT_SIZE = 20L;
    private static final long MAXIMUM_PUT_SIZE = 10L;
    private static final long SMALL_PUT_SIZE = 6L;

    private final String tableName;

    @Parameterized.Parameters(name = "tableName={0}")
    public static Object[] data() {
        return new Object[] {"test", "foo.bar", "[intentionally.invalid.table.name, foo.bar.baz]"};
    }

    public IterablePartitionerTest(String tableName) {
        this.tableName = tableName;
    }

    @SuppressWarnings("Slf4jConstantLogMessage")
    @Test
    public void testWithLogging() {
        Logger mockLogger = Mockito.mock(Logger.class);
        Mockito.when(mockLogger.isWarnEnabled()).thenReturn(true);

        simplePartition(mockLogger, LARGE_PUT_SIZE);

        // verify the correct log messages were sent
        Mockito.verify(mockLogger, Mockito.times(3)).isWarnEnabled();
        Mockito.verify(mockLogger, Mockito.times(3)).warn(
                Mockito.anyString(),
                Mockito.eq(SafeArg.of("approximatePutSize", LARGE_PUT_SIZE)),
                Mockito.eq(SafeArg.of("maximumPutSize", MAXIMUM_PUT_SIZE)),
                Mockito.eq(UnsafeArg.of("tableName", tableName)));
        Mockito.verifyNoMoreInteractions(mockLogger);
    }

    @Test
    public void testWithoutLogging() {
        Logger mockLogger = Mockito.mock(Logger.class);
        Mockito.when(mockLogger.isWarnEnabled()).thenReturn(false);

        simplePartition(mockLogger, LARGE_PUT_SIZE);

        // warn isn't enabled, so it should check 3 times but not log anything
        Mockito.verify(mockLogger, Mockito.times(3)).isWarnEnabled();
        Mockito.verifyNoMoreInteractions(mockLogger);
    }

    @Test
    public void smallPutsDoNotLog() {
        Logger mockLogger = Mockito.mock(Logger.class);
        Mockito.when(mockLogger.isWarnEnabled()).thenReturn(true);

        simplePartition(mockLogger, SMALL_PUT_SIZE);

        // verify the log messages were not sent
        Mockito.verifyNoMoreInteractions(mockLogger);
    }

    private void simplePartition(Logger mockLogger, long approximatePutSize) {
        Iterable<List<Integer>> partitions = IterablePartitioner.partitionByCountAndBytes(
                Lists.newArrayList(1, 2, 3),
                2,
                MAXIMUM_PUT_SIZE,
                tableName,
                (foo) -> approximatePutSize,
                mockLogger
        );
        int i = 1;
        for (List<Integer> partition : partitions) {
            Assert.assertThat(partition, Matchers.contains(i));
            i++;
        }
    }
}

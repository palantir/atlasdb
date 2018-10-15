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
package com.palantir.atlasdb.cleaner;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.sweep.AbstractSweepTaskRunnerTest;
import com.palantir.common.concurrent.PTExecutors;

public class InMemorySweepTaskRunnerTest extends AbstractSweepTaskRunnerTest {
    private ExecutorService exec;

    @Override
    @After
    public void close() {
        super.close();
        exec.shutdown();
    }

    @Override
    protected KeyValueService getKeyValueService() {
        exec = PTExecutors.newCachedThreadPool();
        return new InMemoryKeyValueService(false, exec);
    }

    // This test exists because doing this many writes to a real KVS will likely take too long for tests.
    @Test(timeout = 50000)
    public void testSweepVeryHighlyVersionedCell() {
        createTable(TableMetadataPersistence.SweepStrategy.CONSERVATIVE);

        IntStream.rangeClosed(1, 50_000)
                .forEach(i -> putIntoDefaultColumn("row", RandomStringUtils.random(10), i));
        Optional<SweepResults> results = completeSweep(TABLE_NAME, 100_000, 1);
        Assert.assertEquals(50_000 - 1, results.get().getStaleValuesDeleted());
    }
}

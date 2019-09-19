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

import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.sweep.AbstractSweepTaskRunnerTest;
import java.util.Optional;
import java.util.stream.IntStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

public class InMemorySweepTaskRunnerTest extends AbstractSweepTaskRunnerTest {
    @ClassRule
    public static final TestResourceManager TRM = TestResourceManager.inMemory();

    public InMemorySweepTaskRunnerTest() {
        super(TRM, TRM);
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

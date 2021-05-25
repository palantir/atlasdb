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

package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import com.palantir.atlasdb.keyvalue.dbkvs.DbKvsOracleTestSuite;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class OverflowSequenceSupplierEteTest {
    @ClassRule
    public static final TestResourceManager TRM =
            new TestResourceManager(() -> ConnectionManagerAwareDbKvs.create(DbKvsOracleTestSuite.getKvsConfig()));

    private ExecutorService executor = Executors.newFixedThreadPool(4);
    private static final int THREAD_COUNT = 3;
    private static final int OVERFLOW_IDS_PER_THREAD = 1020;
    private ConnectionSupplier connectionSupplier;

    @Before
    public void setUp() {
        connectionSupplier = DbKvsOracleTestSuite.getConnectionSupplier(TRM.getDefaultKvs());
    }

    @After
    public void tearDown() {
        connectionSupplier.close();
    }

    @Test
    public void getMonotonicallyIncreasingOverflowIdsFromOverflowSequenceSupplierMultiThread()
            throws InterruptedException {
        final Set<Long> overflowIds = new HashSet<>();
        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.execute(() -> getMultipleOverflowIds(overflowIds));
        }
        waitForExecutorToFinish();
    }

    private void getMultipleOverflowIds(Set<Long> overflowIds) {
        final OverflowSequenceSupplier sequenceSupplier = OverflowSequenceSupplier.create(
                connectionSupplier, DbKvsOracleTestSuite.getKvsConfig().ddl().tablePrefix());

        long previousOverflowId = -1;
        for (int j = 0; j < OVERFLOW_IDS_PER_THREAD; j++) {
            long overflowId = sequenceSupplier.get();
            Assert.assertThat(
                    "OverflowIds must always be monotonically increasing.",
                    overflowId,
                    Matchers.greaterThan(previousOverflowId));
            Assert.assertThat(
                    "OverflowIDs must be different across threads.",
                    overflowIds,
                    Matchers.not(Matchers.hasItem(overflowId)));
            overflowIds.add(overflowId);
            previousOverflowId = overflowId;
        }
    }

    private void waitForExecutorToFinish() throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(100, TimeUnit.SECONDS);
    }
}

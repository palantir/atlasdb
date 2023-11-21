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

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManagerV2;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class OverflowSequenceSupplierEteTest {
    @RegisterExtension
    public static final DbKvsOracleExtension dbKvsOracleExtension = new DbKvsOracleExtension();

    @RegisterExtension
    public static final TestResourceManagerV2 TRM =
            new TestResourceManagerV2(() -> ConnectionManagerAwareDbKvs.create(dbKvsOracleExtension.getKvsConfig()));

    private final ExecutorService executor = Executors.newFixedThreadPool(4);
    private static final int THREAD_COUNT = 3;
    private static final int OVERFLOW_IDS_PER_THREAD = 1020;
    private ConnectionSupplier connectionSupplier;

    @BeforeEach
    public void setUp() {
        connectionSupplier = dbKvsOracleExtension.getConnectionSupplier(TRM.getDefaultKvs());
    }

    @AfterEach
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
                connectionSupplier, dbKvsOracleExtension.getKvsConfig().ddl().tablePrefix());

        long previousOverflowId = -1;
        for (int j = 0; j < OVERFLOW_IDS_PER_THREAD; j++) {
            long overflowId = sequenceSupplier.get();
            assertThat(overflowId)
                    .describedAs("OverflowIds must always be monotonically increasing.")
                    .isGreaterThan(previousOverflowId);
            assertThat(overflowIds)
                    .describedAs("OverflowIDs must be different across threads.")
                    .doesNotContain(overflowId);
            overflowIds.add(overflowId);
            previousOverflowId = overflowId;
        }
    }

    private void waitForExecutorToFinish() throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(100, TimeUnit.SECONDS);
    }
}

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
package com.palantir.atlasdb.keyvalue.cassandra;

import com.palantir.atlasdb.cassandra.CassandraMutationTimestampProvider;
import com.palantir.atlasdb.cassandra.CassandraMutationTimestampProviders;
import com.palantir.common.base.Throwables;
import com.palantir.timelock.paxos.InMemoryTimelockServices;
import com.palantir.timestamp.TimestampService;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.rules.TemporaryFolder;

/**
 * Utilities for ETE tests
 * Created by aloro on 12/04/2016.
 */
public final class CassandraTestTools {
    private static final int NUM_PARALLEL_TASKS = 32;

    private CassandraTestTools() {
        // Empty constructor for utility class
    }

    public static void executeInParallelOnExecutorService(Runnable runnable) {
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_PARALLEL_TASKS);
        List<Future<?>> futures = Stream.generate(() -> executorService.submit(runnable))
                .limit(NUM_PARALLEL_TASKS)
                .collect(Collectors.toList());
        futures.forEach(future -> {
            try {
                future.get(1, TimeUnit.MINUTES);
            } catch (InterruptedException | ExecutionException | TimeoutException exception) {
                throw Throwables.rewrapAndThrowUncheckedException(exception);
            }
        });
        executorService.shutdown();
    }

    public static CassandraMutationTimestampProvider getMutationProviderWithStartingTimestamp(
            long timestamp, TemporaryFolder tempFolder) {
        InMemoryTimelockServices timelockServices = InMemoryTimelockServices.create(getNewTempFolder(tempFolder));
        TimestampService timestampService = timelockServices.getTimestampService();
        timelockServices.getTimestampManagementService().fastForwardTimestamp(timestamp);
        return CassandraMutationTimestampProviders.singleLongSupplierBacked(timestampService::getFreshTimestamp);
    }

    private static File getNewTempFolder(TemporaryFolder tempFolder) {
        try {
            return tempFolder.newFolder();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

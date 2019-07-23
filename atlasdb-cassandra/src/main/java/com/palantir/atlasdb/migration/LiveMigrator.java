/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.migration;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Streams;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.base.AbortingVisitor;
import com.palantir.common.base.BatchingVisitable;

public class LiveMigrator {
    private static final Logger log = LoggerFactory.getLogger(LiveMigrator.class);
    private final TransactionManager transactionManager;
    private final TableReference startTable;
    private final TableReference targetTable;
    private final ProgressCheckPoint progressCheckPoint;
    private final ScheduledExecutorService executor;
    private Runnable callback;
    private volatile int batchSize = 1000;

    public LiveMigrator(TransactionManager transactionManager, TableReference startTable,
            TableReference targetTable, ProgressCheckPoint progressCheckPoint, ScheduledExecutorService executor) {
        this.transactionManager = transactionManager;
        this.startTable = startTable;
        this.targetTable = targetTable;
        this.progressCheckPoint = progressCheckPoint;
        this.executor = executor;
    }

    public void startMigration(Runnable callback) {
        this.callback = callback;
        executor.submit(this::runOneIterationSafe);
    }

    private void runOneIterationSafe() {
        try {
            runOneIteration();
        } catch (Throwable th) {
            log.info("Encountered an error running the live migration. Will retry later.", th);
            executor.schedule(this::runOneIterationSafe, 10, TimeUnit.SECONDS);

        }
    }

    private void runOneIteration() {
        Optional<byte[]> nextStartRow = progressCheckPoint.getNextStartRow();
        if (!nextStartRow.isPresent()) {
            callback.run();
            return;
        }

        RangeRequest request = RangeRequest.builder()
                .startRowInclusive(nextStartRow.get())
                .batchHint(batchSize)
                .build();

        Optional<byte[]> lastRead = transactionManager.runTaskWithRetry(
                transaction -> {
                    AtomicReference<byte[]> lastReadRef = new AtomicReference<>();
                    BatchingVisitable<RowResult<byte[]>> range = transaction.getRange(startTable, request);

                    range.batchAccept(batchSize, new AbortingVisitor<List<RowResult<byte[]>>, RuntimeException>() {
                        @Override
                        public boolean visit(List<RowResult<byte[]>> item) {
                            item.forEach(
                                    rowResult -> {
                                        transaction.put(targetTable,
                                                Streams.stream(rowResult.getCells()).collect(
                                                        Collectors.toMap(Map.Entry::getKey,
                                                                Map.Entry::getValue)));
                                        lastReadRef.set(rowResult.getRowName());
                                    });
                            return false;
                        }
                    });

                    return Optional.ofNullable(lastReadRef.get());
                }
        );

        progressCheckPoint.setNextStartRow(lastRead.map(RangeRequests::nextLexicographicName));
        executor.schedule(this::runOneIterationSafe, 10, TimeUnit.SECONDS);
    }

    public void setBatchSize(int n) {
        batchSize = n;
    }

    public boolean isDone() {
        return false;
    }
}

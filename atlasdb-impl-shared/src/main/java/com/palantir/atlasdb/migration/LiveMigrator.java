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
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.google.common.collect.Streams;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.base.AbortingVisitor;
import com.palantir.common.base.BatchingVisitable;

public class LiveMigrator {
    private final TransactionManager transactionManager;
    private final TableReference startTable;
    private final TableReference targetTable;
    private final ProgressCheckPoint progressCheckPoint;

    public LiveMigrator(TransactionManager transactionManager, TableReference startTable,
            TableReference targetTable, ProgressCheckPoint progressCheckPoint) {
        this.transactionManager = transactionManager;
        this.startTable = startTable;
        this.targetTable = targetTable;
        this.progressCheckPoint = progressCheckPoint;
    }

    public void startMigration() {
        KeyValueService keyValueService = transactionManager.getKeyValueService();

        while (true) {
            Optional<byte[]> nextStartRow = progressCheckPoint.getNextStartRow();
            if (!nextStartRow.isPresent()) {
                return;
            }

            RangeRequest request = RangeRequest.builder()
                    .startRowInclusive(nextStartRow.get())
                    .batchHint(1000)
                    .build();

            Optional<byte[]> lastRead = transactionManager.runTaskWithRetry(
                    transaction -> {
                        AtomicReference<byte[]> lastReadRef = new AtomicReference<>();
                        BatchingVisitable<RowResult<byte[]>> range = transaction.getRange(startTable, request);

                        range.batchAccept(1000, new AbortingVisitor<List<RowResult<byte[]>>, RuntimeException>() {
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
                                return true;
                            }
                        });

                        return Optional.ofNullable(lastReadRef.get());
                    }
            );

            progressCheckPoint.setNextStartRow(lastRead);
        }
    }


    public boolean isDone() {
        return false;
    }

}

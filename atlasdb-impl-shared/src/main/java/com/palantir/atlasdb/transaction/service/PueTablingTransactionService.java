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

package com.palantir.atlasdb.transaction.service;

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.pue.ComplexPutUnlessExistsTable;
import com.palantir.atlasdb.keyvalue.pue.PutUnlessExistsTable;
import com.palantir.atlasdb.keyvalue.pue.ValueSerializers;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

// TODO (jkong): I am incomplete
public class PueTablingTransactionService implements TransactionService {
    private final PutUnlessExistsTable<Long> transactionsTable;

    private PueTablingTransactionService(PutUnlessExistsTable<Long> transactionsTable) {
        this.transactionsTable = transactionsTable;
    }

    public static TransactionService createV3(KeyValueService keyValueService) {
        PutUnlessExistsTable<Long> table = ComplexPutUnlessExistsTable.create(
                keyValueService,
                TransactionConstants.TRANSACTIONS3_TABLE,
                ValueSerializers.<Long>builder().build(),
                _unused -> TransactionConstants.FAILED_COMMIT_TS);
        return new PueTablingTransactionService(table);
    }

    @Override
    public ListenableFuture<Long> getAsync(long startTimestamp) {
        return null;
    }

    @Override
    public ListenableFuture<Map<Long, Long>> getAsync(Iterable<Long> startTimestamps) {
        return null;
    }

    @Nullable
    @Override
    public Long get(long startTimestamp) {
        return null;
    }

    @Override
    public Map<Long, Long> get(Iterable<Long> startTimestamps) {
        return null;
    }

    @Override
    public void putUnlessExists(long startTimestamp, long commitTimestamp) throws KeyAlreadyExistsException {}

    @Override
    public void close() {}
}

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
package com.palantir.atlasdb.transaction.impl;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.annotation.Idempotent;

public abstract class AbstractTransaction implements Transaction {
    protected static final ImmutableSortedMap<byte[], RowResult<byte[]>> EMPTY_SORTED_ROWS =
            ImmutableSortedMap.<byte[], RowResult<byte[]>>orderedBy(UnsignedBytes.lexicographicalComparator())
                    .build();

    private TransactionType transactionType = TransactionType.DEFAULT;

    @Override
    @Idempotent
    public final TransactionType getTransactionType() {
        return transactionType;
    }

    @Override
    @Idempotent
    public final void setTransactionType(TransactionType transactionType) {
        this.transactionType = transactionType;
    }

    protected abstract KeyValueService getKeyValueService();

    @Override
    public void commit(TransactionService txService) throws TransactionFailedException {
        commit();
    }
}

/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.transaction.impl;

import java.util.List;

import org.apache.commons.lang.Validate;

import com.google.common.base.Functions;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.base.AbortingVisitor;
import com.palantir.common.base.AbstractBatchingVisitable;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitableFromIterable;
import com.palantir.common.base.BatchingVisitables;
import com.palantir.common.base.ClosableIterator;

/**
 * This will read the values of all committed transactions.
 */
public class ReadOnlyTransaction extends SnapshotTransaction {

    public ReadOnlyTransaction(KeyValueService keyValueService,
                               TransactionService transactionService,
                               long startTimeStamp,
                               AtlasDbConstraintCheckingMode constraintCheckingMode,
                               TransactionReadSentinelBehavior readSentinelBehavior,
                               boolean allowHiddenTableAccess) {
        super(keyValueService,
              transactionService,
              null,
              startTimeStamp,
              constraintCheckingMode,
              readSentinelBehavior,
              allowHiddenTableAccess);
    }

    @Override
    protected boolean shouldDeleteAndRollback() {
        // We don't want to delete any data or roll back any transactions because we don't participate in the
        // transaction protocol.  We just want to skip over anything we find that isn't committed
        return false;
    }

    public BatchingVisitable<RowResult<Value>> getRangeWithTimestamps(final TableReference tableRef,
                                                                      final RangeRequest range) {
        checkGetPreconditions(tableRef);
        if (range.isEmptyRange()) {
            return BatchingVisitables.emptyBatchingVisitable();
        }
        return new AbstractBatchingVisitable<RowResult<Value>>() {
            @Override
            public <K extends Exception> void batchAcceptSizeHint(int userRequestedSize,
                                                                  ConsistentVisitor<RowResult<Value>, K> v)
                    throws K {
                if (range.getBatchHint() != null) {
                    userRequestedSize = range.getBatchHint();
                }

                int preFilterBatchSize = getRequestHintToKvStore(userRequestedSize);

                Validate.isTrue(!range.isReverse(), "we currently do not support reverse ranges");
                getBatchingVisitableFromIterator(
                        tableRef,
                        range,
                        userRequestedSize,
                        v,
                        preFilterBatchSize);
            }

        };
    }

    private <K extends Exception> boolean getBatchingVisitableFromIterator(final TableReference tableRef,
                                                                           RangeRequest range,
                                                                           int userRequestedSize,
                                                                           AbortingVisitor<List<RowResult<Value>>, K> v,
                                                                           int preFilterBatchSize) throws K {
        ClosableIterator<RowResult<Value>> postFilterIterator =
                postFilterIterator(tableRef, range, preFilterBatchSize, Functions.<Value>identity());
        try {
            return BatchingVisitableFromIterable.create(postFilterIterator).batchAccept(userRequestedSize, v);
        } finally {
            postFilterIterator.close();
        }
    }

}

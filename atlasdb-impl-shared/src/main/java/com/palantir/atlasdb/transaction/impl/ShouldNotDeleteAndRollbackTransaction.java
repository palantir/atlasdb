/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.service.TransactionService;

/**
 * This will read the values of all committed transactions.
 */
public class ShouldNotDeleteAndRollbackTransaction extends SnapshotTransaction {

    private static final ExecutorService IGNORING_EXECUTOR = new AbstractExecutorService() {
        @Override
        public void shutdown() {
        }

        @Override
        public List<Runnable> shutdownNow() {
            return null;
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return false;
        }

        @Override
        public void execute(Runnable command) {
            // Should not be called
        }
    };

    public ShouldNotDeleteAndRollbackTransaction(KeyValueService keyValueService,
                               TransactionService transactionService,
                               long startTimeStamp,
                               AtlasDbConstraintCheckingMode constraintCheckingMode,
                               TransactionReadSentinelBehavior readSentinelBehavior,
                               boolean allowHiddenTableAccess,
                               TimestampCache timestampCache,
                               ExecutorService getRangesExecutor,
                               int defaultGetRangesConcurrency) {
        super(keyValueService,
              transactionService,
              null,
              startTimeStamp,
              constraintCheckingMode,
              readSentinelBehavior,
              allowHiddenTableAccess,
              timestampCache,
              // never actually used, since timelockService is null
              AtlasDbConstants.DEFAULT_TRANSACTION_LOCK_ACQUIRE_TIMEOUT_MS,
              getRangesExecutor,
              defaultGetRangesConcurrency,
              IGNORING_EXECUTOR);
    }

    @Override
    protected boolean shouldDeleteAndRollback() {
        // We don't want to delete any data or roll back any transactions because we don't participate in the
        // transaction protocol.  We just want to skip over anything we find that isn't committed
        return false;
    }

}

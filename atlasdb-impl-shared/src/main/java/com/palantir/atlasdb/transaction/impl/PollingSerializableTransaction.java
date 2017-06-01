/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import com.google.common.base.Supplier;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.RemoteLockService;
import com.palantir.timestamp.TimestampService;

public class PollingSerializableTransaction extends SerializableTransaction {
    public PollingSerializableTransaction(KeyValueService keyValueService,
            RemoteLockService lockService, TimestampService timestampService,
            TransactionService transactionService,
            Cleaner cleaner, Supplier<Long> startTimeStamp,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager, long immutableTimestamp,
            Iterable<LockRefreshToken> tokensValidForCommit,
            AtlasDbConstraintCheckingMode constraintCheckingMode,
            Long transactionTimeoutMillis,
            TransactionReadSentinelBehavior readSentinelBehavior,
            boolean allowHiddenTableAccess, TimestampCache timestampCache) {
        super(keyValueService, lockService, timestampService, transactionService, cleaner, startTimeStamp,
                conflictDetectionManager, sweepStrategyManager, immutableTimestamp, tokensValidForCommit,
                constraintCheckingMode, transactionTimeoutMillis, readSentinelBehavior, allowHiddenTableAccess,
                timestampCache);
    }
}

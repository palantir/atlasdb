// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.transaction.impl;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockService;
import com.palantir.timestamp.TimestampService;

public class SerializableTransactionManager extends SnapshotTransactionManager {

    public SerializableTransactionManager(KeyValueService keyValueService,
                                          TimestampService timestampService,
                                          LockClient lockClient,
                                          LockService lockService,
                                          TransactionService transactionService,
                                          Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
                                          ConflictDetectionManager conflictDetectionManager,
                                          Cleaner cleaner) {
        super(
                keyValueService,
                timestampService,
                lockClient,
                lockService,
                transactionService,
                constraintModeSupplier,
                conflictDetectionManager,
                SweepStrategyManagers.createDefault(keyValueService),
                cleaner);
    }

    @Override
    protected SnapshotTransaction createTransaction(long immutableLockTs,
                                                  Supplier<Long> startTimestampSupplier,
                                                  ImmutableList<HeldLocksToken> allTokens) {
        return new SerializableTransaction(
                keyValueService,
                lockService,
                timestampService,
                transactionService,
                cleaner,
                startTimestampSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                getImmutableTimestampInternal(immutableLockTs),
                allTokens,
                constraintModeSupplier.get(),
                cleaner.getTransactionReadTimeoutMillis(),
                TransactionReadSentinelBehavior.THROW_EXCEPTION,
                false);
    }
}


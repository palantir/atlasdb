/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.v2.api.transaction.scanner;

import java.util.Iterator;

import com.palantir.atlasdb.v2.api.AsyncIterator;
import com.palantir.atlasdb.v2.api.AsyncIterators;
import com.palantir.atlasdb.v2.api.NewIds.Table;
import com.palantir.atlasdb.v2.api.NewValue;
import com.palantir.atlasdb.v2.api.NewValue.TransactionValue;
import com.palantir.atlasdb.v2.api.ScanAttributes;
import com.palantir.atlasdb.v2.api.ScanFilter;
import com.palantir.atlasdb.v2.api.transaction.Scanner;
import com.palantir.atlasdb.v2.api.transaction.TransactionState;

public final class MergeInTransactionWritesScanner implements Scanner<NewValue> {
    private final AsyncIterators iterators;
    private final Scanner<NewValue> kvsWritesScanner;

    public MergeInTransactionWritesScanner(
            AsyncIterators iterators,
            Scanner<NewValue> kvsWritesScanner) {
        this.iterators = iterators;
        this.kvsWritesScanner = kvsWritesScanner;
    }

    @Override
    public AsyncIterator<NewValue> scan(
            TransactionState state, Table table, ScanAttributes attributes, ScanFilter filter) {
        AsyncIterator<NewValue> kvsScan = kvsWritesScanner.scan(state, table, attributes, filter);
        Iterator<TransactionValue> transactionScan = state.scan(table, attributes, filter);
        AsyncIterator<NewValue> merged =
                iterators.mergeSorted(attributes.cellComparator(), kvsScan, transactionScan, (kvs, txn) -> txn);
        return iterators.filter(merged, NewValue::isLive);
    }
}

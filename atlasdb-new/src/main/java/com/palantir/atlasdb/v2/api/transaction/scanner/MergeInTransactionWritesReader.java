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
import com.palantir.atlasdb.v2.api.NewValue;
import com.palantir.atlasdb.v2.api.NewValue.TransactionValue;
import com.palantir.atlasdb.v2.api.ScanDefinition;
import com.palantir.atlasdb.v2.api.transaction.Reader;
import com.palantir.atlasdb.v2.api.transaction.state.TransactionState;

public final class MergeInTransactionWritesReader implements Reader<NewValue> {
    private final AsyncIterators iterators;
    private final Reader<? extends NewValue> kvsWritesReader;

    public MergeInTransactionWritesReader(
            AsyncIterators iterators,
            Reader<? extends NewValue> kvsWritesReader) {
        this.iterators = iterators;
        this.kvsWritesReader = kvsWritesReader;
    }

    @Override
    public AsyncIterator<NewValue> scan(TransactionState state, ScanDefinition definition) {
        AsyncIterator<? extends NewValue> kvsScan = kvsWritesReader.scan(state, definition);
        Iterator<TransactionValue> transactionScan =
                state.scan(definition.table(), definition.attributes(), definition.filter());
        AsyncIterator<NewValue> merged = iterators.mergeSorted(
                definition.attributes().cellComparator(), kvsScan, transactionScan, (kvs, txn) -> txn);
        return iterators.filter(merged, NewValue::isLive);
    }
}

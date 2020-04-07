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
import java.util.List;

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.v2.api.AsyncIterator;
import com.palantir.atlasdb.v2.api.AsyncIterators;
import com.palantir.atlasdb.v2.api.NewIds;
import com.palantir.atlasdb.v2.api.NewValue.CommittedValue;
import com.palantir.atlasdb.v2.api.NewValue.KvsValue;
import com.palantir.atlasdb.v2.api.ScanAttributes;
import com.palantir.atlasdb.v2.api.ScanFilter;
import com.palantir.atlasdb.v2.api.kvs.Kvs;
import com.palantir.atlasdb.v2.api.transaction.Scanner;
import com.palantir.atlasdb.v2.api.transaction.TransactionState;

public final class FilterUncommittedTransactionDataScanner implements Scanner<CommittedValue> {
    private final AsyncIterators iterators;
    private final Kvs delegate;

    public FilterUncommittedTransactionDataScanner(
            AsyncIterators iterators,
            Kvs delegate) {
        this.iterators = iterators;
        this.delegate = delegate;
    }

    @Override
    public AsyncIterator<CommittedValue> scan(
            TransactionState state, NewIds.Table table, ScanAttributes attributes, ScanFilter filter) {
        AsyncIterator<KvsValue> scan = delegate.scan(state, table, attributes, filter);
        AsyncIterator<List<KvsValue>> buffered = iterators.nonBlockingPages(scan);

        return iterators.concat(iterators.transformAsync(buffered, this::postFilterWrites));
    }

    private ListenableFuture<Iterator<CommittedValue>> postFilterWrites(List<KvsValue> values) {
        return null;
    }
}

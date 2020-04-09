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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.v2.api.iterators.AsyncIterators;
import com.palantir.atlasdb.v2.api.api.NewValue;
import com.palantir.atlasdb.v2.api.api.ScanDefinition;
import com.palantir.atlasdb.v2.api.api.NewLocks;
import com.palantir.atlasdb.v2.api.transaction.state.TransactionState;

public final class CheckImmutableLocksReader<T extends NewValue> extends TransformingReader<T, T> {
    private final NewLocks locks;

    public CheckImmutableLocksReader(Reader<T> delegate, AsyncIterators iterators, NewLocks locks) {
        super(delegate, iterators);
        this.locks = locks;
    }

    @Override
    protected ListenableFuture<Iterator<T>> transformPage(
            TransactionState state, ScanDefinition definition, List<T> page) {
        return Futures.transform(
                locks.checkStillValid(state.heldLocks()),
                $ -> page.iterator(),
                MoreExecutors.directExecutor());
    }
}

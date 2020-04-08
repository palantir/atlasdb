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

package com.palantir.atlasdb.v2.api.transaction.state;

import static com.palantir.logsafe.Preconditions.checkNotNull;

import java.util.function.Consumer;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.v2.api.iterators.AsyncIterator;

public final class StateHolder {
    private TransactionState state;

    public StateHolder(TransactionState state) {
        this.state = state;
    }

    public TransactionState invalidateAndGet() {
        TransactionState snapshot = get();
        state = null;
        return snapshot;
    }

    public void mutate(Consumer<TransactionState.Builder> mutator) {
        TransactionState.Builder builder = get().toBuilder();
        mutator.accept(builder);
        state = builder.build();
    }

    // this method feels ugly
    public <T extends Consumer<TransactionState.Builder>> AsyncIterator<T> iterate(
            AsyncIterator<T> iterator,
            Consumer<TransactionState.Builder> ifAtEnd) {
        return new AsyncIterator<T>() {
            @Override
            public ListenableFuture<Boolean> onHasNext() {
                return Futures.transform(iterator.onHasNext(), hasNext -> {
                    if (!hasNext) {
                        mutate(ifAtEnd);
                    }
                    return hasNext;
                }, MoreExecutors.directExecutor());
            }

            @Override
            public boolean hasNext() {
                boolean hasNext = iterator.hasNext();
                if (!hasNext) {
                    // this can easily be called many times...
                    mutate(ifAtEnd);
                }
                return hasNext;
            }

            @Override
            public T next() {
                T next = iterator.next();
                mutate(next);
                return next;
            }
        };
    }

    public TransactionState get() {
        return checkNotNull(state);
    }
}

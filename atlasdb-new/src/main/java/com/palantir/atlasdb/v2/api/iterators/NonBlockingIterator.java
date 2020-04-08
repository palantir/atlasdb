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

package com.palantir.atlasdb.v2.api.iterators;

import java.util.Iterator;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

public class NonBlockingIterator<T> implements AsyncIterator<T> {
    private final ListenableFuture<Iterator<T>> nonBlockingDelegate;

    public NonBlockingIterator(ListenableFuture<Iterator<T>> nonBlockingDelegate) {
        this.nonBlockingDelegate = nonBlockingDelegate;
    }

    @Override
    public ListenableFuture<Boolean> onHasNext() {
        return Futures.transform(nonBlockingDelegate, Iterator::hasNext, MoreExecutors.directExecutor());
    }

    @Override
    public boolean hasNext() {
        return Futures.getUnchecked(nonBlockingDelegate).hasNext();
    }

    @Override
    public T next() {
        return Futures.getUnchecked(nonBlockingDelegate).next();
    }
}

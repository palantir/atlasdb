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

package com.palantir.atlasdb.v2.api.api;

import java.util.Iterator;
import java.util.function.Consumer;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

// mid-term intention - do something magical where you maintain a stack of transformations and so don't wrap iterators
// - basically avoid executor scheduling penalty by processing as much as possible synchronously and buffering as
// little as possible
public interface AsyncIterator<T> extends Iterator<T> {
    /**
     * Returns an asynchronous signal for the next iteration of hasNext. Once the future is complete, hasNext and
     * next will not block.
     */
    ListenableFuture<Boolean> onHasNext();

    default void forEachRemainingWhileNonBlocking(Consumer<T> consumer) {
        for (ListenableFuture<Boolean> hasNextAsync = onHasNext(); hasNextAsync.isDone(); hasNextAsync = onHasNext()) {
            boolean hasNext = Futures.getUnchecked(hasNextAsync);
            if (!hasNext) {
                return;
            }
            consumer.accept(next());
        }
    }
}

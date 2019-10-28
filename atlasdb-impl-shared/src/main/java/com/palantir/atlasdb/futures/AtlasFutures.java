/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.futures;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.common.base.Throwables;

public final class AtlasFutures {
    private AtlasFutures() {

    }

    public static <R> R getUnchecked(ListenableFuture<R> listenableFuture) {
        try {
            return listenableFuture.get();
        } catch (ExecutionException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e.getCause());
        } catch (Exception e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    public static <R> R getDone(ListenableFuture<R> resultFuture) {
        try {
            return Futures.getDone(resultFuture);
        } catch (ExecutionException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e.getCause());
        }
    }

    public interface FutureCallable<R> extends Callable<ListenableFuture<R>> {

    }
}

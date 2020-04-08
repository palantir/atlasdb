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

package com.palantir.atlasdb.v2.api.future;

import java.util.concurrent.Executor;
import java.util.function.Predicate;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class FutureWhile {
    private FutureWhile() {}

    public static <T> ListenableFuture<T> whileTrue(
            Executor executor,
            ListenableFuture<T> initialState,
            AsyncFunction<T, T> transformer,
            Predicate<T> stopIfFalse) {
        return Futures.transformAsync(
                initialState,
                result -> {
                    if (!stopIfFalse.test(result)) {
                        return Futures.immediateFuture(result);
                    } else {
                        return whileTrue(executor,
                                Futures.transformAsync(initialState, transformer, executor),
                                transformer,
                                stopIfFalse);
                    }
                },
                executor);
    }
}

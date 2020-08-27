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

import com.google.common.util.concurrent.ListenableFuture;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * Interface which provides a wrapping over static methods in {@link AtlasFutures} which need an
 * {@link ExecutorService}. This interface is useful when considering how to easily have a custom resource management
 * with minimal changes to the existing code.
 */
public interface FuturesCombiner extends AutoCloseable {

    /**
     * Wraps the {@link AtlasFutures#allAsMap(Map, ExecutorService)}.
     */
    <T, R> ListenableFuture<Map<T, R>> allAsMap(Map<T, ListenableFuture<Optional<R>>> inputToListenableFutureMap);

    @Override
    void close();
}

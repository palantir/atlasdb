/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.asts;

import java.util.function.Function;
import java.util.stream.Stream;

// Came out of making ShardedSweepableBucketRetriever way easier to test. It was a pain to test
// concurrency logic with no way of controlling the task from the test
public interface ParallelTaskExecutor {
    <V, K> Stream<V> execute(Stream<K> arg, Function<K, V> task, int maxParallelism);
}

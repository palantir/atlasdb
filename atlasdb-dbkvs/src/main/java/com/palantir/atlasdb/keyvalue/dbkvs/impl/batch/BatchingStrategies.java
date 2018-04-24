/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.dbkvs.impl.batch;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterables;
import com.palantir.common.collect.Maps2;

public final class BatchingStrategies {

    private BatchingStrategies() {}

    @SuppressWarnings("unchecked")
    public static <T> BatchingTaskRunner.BatchingStrategy<Iterable<T>> forIterable() {
        return (IterableBatchingStrategy<T>) iterableBatchingStrategy;
    }

    @SuppressWarnings("unchecked")
    public static <T> BatchingTaskRunner.BatchingStrategy<List<T>> forList() {
        return (ListBatchingStrategy<T>) listBatchingStrategy;
    }

    @SuppressWarnings("unchecked")
    public static <K, V> BatchingTaskRunner.BatchingStrategy<Map<K, V>> forMap() {
        return (MapBatchingStrategy<K, V>) mapBatchingStrategy;
    }

    private static class IterableBatchingStrategy<T> implements BatchingTaskRunner.BatchingStrategy<Iterable<T>> {
        @Override
        public Iterable<List<T>> partitionIntoBatches(Iterable<T> collection, int batchSizeHint) {
            return Iterables.partition(collection, batchSizeHint);
        }
    }

    private static final IterableBatchingStrategy<?> iterableBatchingStrategy = new IterableBatchingStrategy<>();

    private static class ListBatchingStrategy<T> implements BatchingTaskRunner.BatchingStrategy<List<T>> {
        @Override
        public Iterable<List<T>> partitionIntoBatches(List<T> collection, int batchSizeHint) {
            return Iterables.partition(collection, batchSizeHint);
        }
    }

    private static final ListBatchingStrategy<?> listBatchingStrategy = new ListBatchingStrategy<>();

    private static class MapBatchingStrategy<K, V> implements BatchingTaskRunner.BatchingStrategy<Map<K, V>> {
        @Override
        public Iterable<? extends Map<K, V>> partitionIntoBatches(Map<K, V> collection, int batchSizeHint) {
            return Iterables.transform(
                    Iterables.partition(collection.entrySet(), batchSizeHint),
                    Maps2::fromEntries);
        }
    }

    private static final MapBatchingStrategy<?, ?> mapBatchingStrategy = new MapBatchingStrategy<>();

}

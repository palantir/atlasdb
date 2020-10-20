/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.batch;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.HashMap;
import java.util.Map;

public final class AccumulatorStrategies {

    private AccumulatorStrategies() {}

    @SuppressWarnings("unchecked")
    public static <K, V> BatchingTaskRunner.ResultAccumulatorStrategy<Map<K, V>> forMap() {
        return (MapAccumulatorStrategy<K, V>) mapAccumulatorStrategy;
    }

    @SuppressWarnings("unchecked")
    public static <K, V> BatchingTaskRunner.ResultAccumulatorStrategy<Multimap<K, V>> forListMultimap() {
        return (ListMultimapAccumulatorStrategy<K, V>) listMultimapAccumulatorStrategy;
    }

    private static final class MapAccumulatorStrategy<K, V>
            implements BatchingTaskRunner.ResultAccumulatorStrategy<Map<K, V>> {
        @Override
        public Map<K, V> createEmptyResult() {
            return new HashMap<>();
        }

        @Override
        public void accumulateResult(Map<K, V> result, Map<K, V> toAdd) {
            result.putAll(toAdd);
        }
    }

    private static final MapAccumulatorStrategy<?, ?> mapAccumulatorStrategy = new MapAccumulatorStrategy<>();

    private static final class ListMultimapAccumulatorStrategy<K, V>
            implements BatchingTaskRunner.ResultAccumulatorStrategy<Multimap<K, V>> {
        @Override
        public Multimap<K, V> createEmptyResult() {
            return ArrayListMultimap.create();
        }

        @Override
        public void accumulateResult(Multimap<K, V> result, Multimap<K, V> toAdd) {
            result.putAll(toAdd);
        }
    }

    private static final ListMultimapAccumulatorStrategy<?, ?> listMultimapAccumulatorStrategy =
            new ListMultimapAccumulatorStrategy<>();
}

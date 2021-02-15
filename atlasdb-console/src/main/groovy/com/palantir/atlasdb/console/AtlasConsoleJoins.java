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
package com.palantir.atlasdb.console;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Utility for joins.
 */
public class AtlasConsoleJoins {

    public enum JoinComponent {
        JOIN_KEY,
        INPUT_VALUE,
        OUTPUT_VALUE
    }

    public static Iterable<Map<String, Object>> join(
            Iterable<Map<?, ?>> input, int batchSize, Function<List<?>, List<Map<String, ?>>> getRowsFunction) {
        Iterable<Entry<?, ?>> entries = Iterables.concat(Iterables.transform(input, Map::entrySet));
        return FluentIterable.from(Iterables.partition(entries, batchSize)).transformAndConcat(batch -> {
            List<?> keys = batch.stream().map(Entry::getKey).collect(Collectors.toList());
            List<Map<String, ?>> batchResult = getRowsFunction.apply(keys);
            Map<?, ?> resultMap = Maps.uniqueIndex(batchResult, m -> m.get("row"));
            return batch.stream()
                    .filter(inputItem -> resultMap.containsKey(inputItem.getKey()))
                    .map(batchItem -> {
                        Map<String, Object> map = Maps.newHashMap();
                        Object row = batchItem.getKey();
                        map.put(JoinComponent.JOIN_KEY.toString(), row);
                        map.put(JoinComponent.OUTPUT_VALUE.toString(), resultMap.get(row));
                        map.put(JoinComponent.INPUT_VALUE.toString(), batchItem.getValue());
                        return map;
                    })
                    .collect(Collectors.toList());
        });
    }
}

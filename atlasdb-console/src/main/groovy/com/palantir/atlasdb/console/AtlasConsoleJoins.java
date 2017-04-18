/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.console;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

/**
 * Created by dcohen on 4/17/17.
 */
public class AtlasConsoleJoins {

    public enum JoinComponent {
        JOIN_KEY, INPUT_VALUE, OUTPUT_VALUE
    }

    public static Iterable<Map<String, Object>> join(Iterable<Map<?, ?>> input, int batchSize,
            Function<List<?>, List<Map<String, ?>>> getRowsFunction) {
        Iterable<Map.Entry<?, ?>> entries = Iterables.concat(Iterables.transform(input, java.util.Map::entrySet));
        return FluentIterable.from(Iterables.partition(entries, batchSize)).transformAndConcat(
                batch -> {
                    Map<?, ?> batchMap = batch.stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
                    List<?> keys = batch.stream().map(Map.Entry::getKey).collect(Collectors.toList());
                    List<Map<String, ?>> batchResult = getRowsFunction.apply(keys);

                    return batchResult.stream().map(result -> {
                        Map<String, Object> map = Maps.newHashMap();
                        Object row = result.get("row");
                        map.put(JoinComponent.JOIN_KEY.toString(), row);
                        map.put(JoinComponent.OUTPUT_VALUE.toString(), result);
                        map.put(JoinComponent.INPUT_VALUE.toString(), batchMap.get(row));
                        return map;
                    }).collect(Collectors.toList());
                }
        );
    }
}

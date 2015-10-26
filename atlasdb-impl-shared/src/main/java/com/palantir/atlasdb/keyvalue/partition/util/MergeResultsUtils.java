/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.partition.util;

import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Value;


public class MergeResultsUtils {

    private static void mergeLatestTimestampMapIntoMap(Map<Cell, Long> globalResult,
                                                Map<Cell, Long> partResult) {
        for (Map.Entry<Cell, Long> e : partResult.entrySet()) {
            if (!globalResult.containsKey(e.getKey())
                    || globalResult.get(e.getKey()) < e.getValue()) {
                globalResult.put(e.getKey(), e.getValue());
            }
        }
    }

    public static Function<Map<Cell, Long>, Void> newLatestTimestampMapMerger(final Map<Cell, Long> result) {
        return new Function<Map<Cell,Long>, Void>() {
            @Override
            public Void apply(@Nullable Map<Cell, Long> input) {
                mergeLatestTimestampMapIntoMap(result, input);
                return null;
            }
        };
    }

    private static void mergeAllTimestampsMapIntoMap(Multimap<Cell, Long> globalResult,
                                              Multimap<Cell, Long> partResult) {
        for (Map.Entry<Cell, Long> e : partResult.entries()) {
            if (!globalResult.containsEntry(e.getKey(), e.getValue())) {
                globalResult.put(e.getKey(), e.getValue());
            }
        }
    }

    public static Function<Multimap<Cell, Long>, Void> newAllTimestampsMapMerger(final Multimap<Cell, Long> result) {
        return new Function<Multimap<Cell, Long>, Void>() {
            @Override
            public Void apply(@Nullable Multimap<Cell, Long> input) {
                mergeAllTimestampsMapIntoMap(result, input);
                return null;
            }

        };
    }

    private static void mergeCellValueMapIntoMap(Map<Cell, Value> globalResult, Map<Cell, Value> partResult) {
        for (Map.Entry<Cell, Value> e : partResult.entrySet()) {
            if (!globalResult.containsKey(e.getKey())
                    || globalResult.get(e.getKey()).getTimestamp() < e.getValue().getTimestamp()) {
                globalResult.put(e.getKey(), e.getValue());
            }
        }
    }

    public static Function<Map<Cell, Value>, Void> newCellValueMapMerger(final Map<Cell, Value> result) {
        return new Function<Map<Cell, Value>, Void>() {
            @Override
            public Void apply(Map<Cell, Value> input) {
                mergeCellValueMapIntoMap(result, input);
                return null;
            }
        };
    }

}

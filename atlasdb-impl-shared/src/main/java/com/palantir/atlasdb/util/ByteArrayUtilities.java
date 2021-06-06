/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.util;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.SortedMap;

public final class ByteArrayUtilities {

    private ByteArrayUtilities() {
        // no-op
    }

    public static boolean areMapsEqual(Map<Cell, byte[]> map1, Map<Cell, byte[]> map2) {
        if (map1.size() != map2.size()) {
            return false;
        }
        for (Map.Entry<Cell, byte[]> e : map1.entrySet()) {
            if (!map2.containsKey(e.getKey())) {
                return false;
            }
            if (UnsignedBytes.lexicographicalComparator().compare(e.getValue(), map2.get(e.getKey())) != 0) {
                return false;
            }
        }
        return true;
    }

    private static boolean areByteMapsEqual(SortedMap<byte[], byte[]> map1, SortedMap<byte[], byte[]> map2) {
        if (map1.size() != map2.size()) {
            return false;
        }
        for (Map.Entry<byte[], byte[]> e : map1.entrySet()) {
            if (!map2.containsKey(e.getKey())) {
                return false;
            }
            if (UnsignedBytes.lexicographicalComparator().compare(e.getValue(), map2.get(e.getKey())) != 0) {
                return false;
            }
        }
        return true;
    }

    public static boolean areRowResultsEqual(
            NavigableMap<byte[], RowResult<byte[]>> first, NavigableMap<byte[], RowResult<byte[]>> second) {
        if (first.size() != second.size()) {
            return false;
        }

        for (Map.Entry<byte[], RowResult<byte[]>> e : first.entrySet()) {
            if (!second.containsKey(e.getKey())) {
                return false;
            }
            SortedMap<byte[], byte[]> firstColumns = e.getValue().getColumns();
            SortedMap<byte[], byte[]> secondColumns = Optional.ofNullable(second.get(e.getKey()))
                    .map(RowResult::getColumns)
                    .orElseGet(ImmutableSortedMap::of);

            return areByteMapsEqual(firstColumns, secondColumns);
        }
        return true;
    }
}

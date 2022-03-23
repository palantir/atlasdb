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
package com.palantir.atlasdb.table.generation;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.common.persist.Persistable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public final class Columns {

    private Columns() {
        // should not be instantiated
    }

    public static <T extends Persistable, V extends Persistable> Set<Cell> toCells(Multimap<T, V> map) {
        Set<Cell> ret = Sets.newHashSetWithExpectedSize(map.size());
        for (Map.Entry<T, Collection<V>> e : map.asMap().entrySet()) {
            byte[] rowName = e.getKey().persistToBytes();
            for (Persistable val : e.getValue()) {
                ret.add(Cell.create(rowName, val.persistToBytes()));
            }
        }
        return ret;
    }
}

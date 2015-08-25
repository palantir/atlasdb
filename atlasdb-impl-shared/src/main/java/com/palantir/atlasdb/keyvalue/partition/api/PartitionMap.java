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
package com.palantir.atlasdb.keyvalue.partition.api;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.partition.VersionedKeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.util.ConsistentRingRangeRequest;

public interface PartitionMap {

    Multimap<ConsistentRingRangeRequest, VersionedKeyValueEndpoint> getServicesForRangeRead(String tableName, RangeRequest range);

    <R> R runForRowsRead(String tableName, Iterable<byte[]> rows, Function<Entry<KeyValueService, Iterable<byte[]>>, R> task);

    <R> R runForCellsRead(String tableName, Set<Cell> cells, Function<Entry<KeyValueService, Set<Cell>>, R> task);
    <T, R> R runForCellsRead(String tableName, Map<Cell, T> cells, Function<Entry<KeyValueService, Map<Cell, T>>, R> task);

    <R> R runForCellsWrite(String tableName, Set<Cell> cells, Function<Entry<KeyValueService, Set<Cell>>, R> task);
    <T, R> R runForCellsWrite(String tableName, Map<Cell, T> cells, Function<Entry<KeyValueService, Map<Cell, T>>, R> task);
    <T, R> R runForCellsWrite(String tableName, Multimap<Cell, T> cells, Function<Entry<KeyValueService, Multimap<Cell, T>>, R> task);

    Set<? extends KeyValueService> getDelegates();

}

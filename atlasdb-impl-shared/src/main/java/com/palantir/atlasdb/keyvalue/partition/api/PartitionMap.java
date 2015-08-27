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
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.google.common.base.Function;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.partition.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.util.ConsistentRingRangeRequest;
import com.palantir.util.Pair;

@JsonTypeInfo(use=Id.CLASS, property="@class")
public interface PartitionMap {

    // This function is a special case as the operations will be carried out at a later time and
    // initiated by the caller.
    Multimap<ConsistentRingRangeRequest, KeyValueEndpoint> getServicesForRangeRead(String tableName, RangeRequest range);

    void runForRowsRead(String tableName, Iterable<byte[]> rows, Function<Pair<KeyValueService, Iterable<byte[]>>, Void> task);

    void runForCellsRead(String tableName, Set<Cell> cells, Function<Pair<KeyValueService, Set<Cell>>, Void> task);
    <T> void runForCellsRead(String tableName, Map<Cell, T> cells, Function<Pair<KeyValueService, Map<Cell, T>>, Void> task);

    void runForCellsWrite(String tableName, Set<Cell> cells, Function<Pair<KeyValueService, Set<Cell>>, Void> task);
    <T> void runForCellsWrite(String tableName, Map<Cell, T> cells, Function<Pair<KeyValueService, Map<Cell, T>>, Void> task);
    <T> void runForCellsWrite(String tableName, Multimap<Cell, T> cells, Function<Pair<KeyValueService, Multimap<Cell, T>>, Void> task);

    Set<? extends KeyValueService> getDelegates();

}

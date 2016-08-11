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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Function;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.exception.ClientVersionTooOldException;
import com.palantir.atlasdb.keyvalue.partition.map.DynamicPartitionMapImpl;
import com.palantir.atlasdb.keyvalue.partition.util.ConsistentRingRangeRequest;
import com.palantir.util.Pair;

@JsonSerialize(using = DynamicPartitionMapImpl.Serializer.class)
@JsonDeserialize(using = DynamicPartitionMapImpl.Deserializer.class)
public interface PartitionMap {
    /**
     * This will divide the <code>range</code> into subranges that are relevant to particular
     * endpoint sets. The returned endpoints might throw <code>VersionTooOldException</code> at
     * any time.
     * Note that the endpoint sets are overlapping due to replication.
     *
     * @see ClientVersionTooOldException
     *
     * @return The iteration order of entries is guaranteed to be non-descending. The sub-ranges
     * must be either equal or non-overlapping. The union of ranges must be a superset of the original
     * <code>range</code>
     */
    Multimap<ConsistentRingRangeRequest, KeyValueEndpoint> getServicesForRangeRead(
            String tableName,
            RangeRequest range);

    /**
     * The <code>task</code> will be executed for all relevant KeyValueServices. The right hand side argument
     * will contain the rows that are relevant to given KeyValueService.
     */
    void runForRowsRead(
            String tableName, Iterable<byte[]> rows,
            Function<Pair<KeyValueService, Iterable<byte[]>>, Void> task);

    /**
     * The <code>task</code> will be executed for all relevant KeyValueServices. The right hand side argument
     * will contain the cells that are relevant to given KeyValueService.
     */
    void runForCellsRead(String tableName, Set<Cell> cells, Function<Pair<KeyValueService, Set<Cell>>, Void> task);

    /**
     * The <code>task</code> will be executed for all relevant KeyValueServices. The right hand side argument
     * will contain a subset of entries with cells relevant to given KeyValueService.
     */
    <T> void runForCellsRead(
            String tableName,
            Map<Cell, T> cells, Function<Pair<KeyValueService, Map<Cell, T>>, Void> task);

    /**
     * The <code>task</code> will be executed for all relevant KeyValueServices. The right hand side argument
     * will contain all the cells that are relevant to given KeyValueService.
     */
    void runForCellsWrite(String tableName, Set<Cell> cells, Function<Pair<KeyValueService, Set<Cell>>, Void> task);

    /**
     * The <code>task</code> will be executed for all relevant KeyValueServices. The right hand side argument
     * will contain a subset of entries with cells relevant to given KeyValueService.
     */
    <T> void runForCellsWrite(
            String tableName,
            Map<Cell, T> cells, Function<Pair<KeyValueService, Map<Cell, T>>, Void> task);

    /**
     * The <code>task</code> will be executed for all relevant KeyValueServices. The right hand side argument
     * will contain a subset of entries with cells relevant to given KeyValueService.
     */
    <T> void runForCellsWrite(
            String tableName,
            Multimap<Cell, T> cells,
            Function<Pair<KeyValueService, Multimap<Cell, T>>, Void> task);

    /**
     * This will return the references to underlying endpoint key value services.
     *
     * @return the references to underlying endpoint key value services
     */
    Set<? extends KeyValueService> getDelegates();

}

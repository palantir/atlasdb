/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.palantir.atlasdb.keyvalue.api.Cell;

class HostPartitioner<V> {
    private final CassandraClientPool cassandraClientPool;

    HostPartitioner(CassandraClientPool cassandraClientPool) {
        this.cassandraClientPool = cassandraClientPool;
    }

    public Map<InetSocketAddress, Map<Cell, V>> partitionMapByHost(Iterable<Map.Entry<Cell, V>> cells) {
        Map<InetSocketAddress, List<Map.Entry<Cell, V>>> partitionedByHost =
                partitionByHost(cells, entry -> entry.getKey().getRowName());
        Map<InetSocketAddress, Map<Cell, V>> cellsByHost = Maps.newHashMap();
        for (Map.Entry<InetSocketAddress, List<Map.Entry<Cell, V>>> hostAndCells : partitionedByHost.entrySet()) {
            Map<Cell, V> cellsForHost = Maps.newHashMapWithExpectedSize(hostAndCells.getValue().size());
            for (Map.Entry<Cell, V> entry : hostAndCells.getValue()) {
                cellsForHost.put(entry.getKey(), entry.getValue());
            }
            cellsByHost.put(hostAndCells.getKey(), cellsForHost);
        }
        return cellsByHost;
    }

    public <V> Map<InetSocketAddress, List<V>> partitionByHost(
            Iterable<V> iterable,
            Function<V, byte[]> keyExtractor) {
        // Ensure that the same key goes to the same partition. This is important when writing multiple columns
        // to the same row, since this is a normally a single write in cassandra, whereas splitting the columns
        // into different requests results in multiple writes.
        ListMultimap<ByteBuffer, V> partitionedByKey = ArrayListMultimap.create();
        for (V value : iterable) {
            partitionedByKey.put(ByteBuffer.wrap(keyExtractor.apply(value)), value);
        }
        ListMultimap<InetSocketAddress, V> valuesByHost = ArrayListMultimap.create();
        for (ByteBuffer key : partitionedByKey.keySet()) {
            InetSocketAddress host = cassandraClientPool.getRandomHostForKey(key.array());
            valuesByHost.putAll(host, partitionedByKey.get(key));
        }
        return Multimaps.asMap(valuesByHost);
    }
}

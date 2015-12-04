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
package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.keyvalue.cassandra.CQLKeyValueServices.TransactionType;
import com.palantir.atlasdb.keyvalue.cassandra.jmx.CassandraJmxCompactionManager;
import com.palantir.atlasdb.keyvalue.cassandra.jmx.CassandraJmxCompactionModule;
import com.palantir.common.base.Throwables;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ExpiringKeyValueService;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;


public class CQLExpiringKeyValueService extends CQLKeyValueService implements ExpiringKeyValueService {

    public static CQLExpiringKeyValueService create(CassandraKeyValueServiceConfigManager configManager) {
        Preconditions.checkState(!configManager.getConfig().servers().isEmpty(), "address list was empty");

        Optional<CassandraJmxCompactionManager> compactionManager = new CassandraJmxCompactionModule().createCompactionManager(configManager);
        CQLExpiringKeyValueService kvs = new CQLExpiringKeyValueService(configManager, compactionManager);
        kvs.initializeConnectionPool();
        kvs.performInitialSetup();
        return kvs;
    }

    private CQLExpiringKeyValueService(CassandraKeyValueServiceConfigManager configManager,
                                       Optional<CassandraJmxCompactionManager> compactionManager) {
        super(configManager, compactionManager);
    }

    @Override
    public void put(final String tableName, final Map<Cell, byte[]> values, final long timestamp, final long time, final TimeUnit unit) {
        try {
            putInternal(tableName, KeyValueServices.toConstantTimestampValues(values.entrySet(), timestamp), TransactionType.NONE, CassandraKeyValueServices.convertTtl(time, unit));
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public void putWithTimestamps(String tableName, Multimap<Cell, Value> values, final long time, final TimeUnit unit) {
        try {
            putInternal(tableName, values.entries(), TransactionType.NONE, CassandraKeyValueServices.convertTtl(time, unit));
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public void multiPut(Map<String, ? extends Map<Cell, byte[]>> valuesByTable, final long timestamp, final long time, final TimeUnit unit) throws KeyAlreadyExistsException {
        Map<ResultSetFuture, String> resultSetFutures = Maps.newHashMap();
        for (Entry<String, ? extends Map<Cell, byte[]>> e : valuesByTable.entrySet()) {
            final String table = e.getKey();
            // We sort here because some key value stores are more efficient if you store adjacent keys together.
            NavigableMap<Cell, byte[]> sortedMap = ImmutableSortedMap.copyOf(e.getValue());


            Iterable<List<Entry<Cell, byte[]>>> partitions = partitionByCountAndBytes(sortedMap.entrySet(),
                    getMultiPutBatchCount(), getMultiPutBatchSizeBytes(), table, CQLKeyValueServices.MULTIPUT_ENTRY_SIZING_FUNCTION);


            for (final List<Entry<Cell, byte[]>> p : partitions) {
                List<Entry<Cell, Value>> partition = Lists.transform(p, new Function<Entry<Cell, byte[]>, Entry<Cell, Value>>() {
                    @Override
                    public Entry<Cell, Value> apply(Entry<Cell, byte[]> input) {
                        return Maps.immutableEntry(input.getKey(), Value.create(input.getValue(), timestamp));
                    }});
                resultSetFutures.put(getPutPartitionResultSetFuture(table, partition, TransactionType.NONE,  CassandraKeyValueServices.convertTtl(time, unit)), table);
            }
        }

        for (Entry<ResultSetFuture, String> result : resultSetFutures.entrySet()) {
            ResultSet resultSet;
            try {
                resultSet = result.getKey().getUninterruptibly();
                resultSet.all();
            } catch (Throwable t) {
                throw Throwables.throwUncheckedException(t);
            }
            CQLKeyValueServices.logTracedQuery(getPutQuery(result.getValue(), CassandraKeyValueServices.convertTtl(time, unit)), resultSet, session, cqlStatementCache.NORMAL_QUERY);
        }
    }

}

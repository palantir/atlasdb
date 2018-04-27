/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ExpiringKeyValueService;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.CqlKeyValueServices.TransactionType;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
import com.palantir.common.base.Throwables;


public final class CqlExpiringKeyValueService extends CqlKeyValueService implements ExpiringKeyValueService {

    public static CqlExpiringKeyValueService create(CassandraKeyValueServiceConfigManager configManager) {
        Preconditions.checkState(!configManager.getConfig().servers().isEmpty(), "address list was empty");

        CqlExpiringKeyValueService kvs = new CqlExpiringKeyValueService(configManager);
        kvs.initializeConnectionPool();
        kvs.performInitialSetup();
        return kvs;
    }

    private CqlExpiringKeyValueService(CassandraKeyValueServiceConfigManager configManager) {
        super(configManager);
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp, long time, TimeUnit unit) {
        try {
            putInternal(
                    tableRef,
                    KeyValueServices.toConstantTimestampValues(values.entrySet(), timestamp),
                    TransactionType.NONE,
                    CassandraKeyValueServices.convertTtl(time, unit),
                    false);
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> values, long time, TimeUnit unit) {
        try {
            putInternal(
                    tableRef,
                    values.entries(),
                    TransactionType.NONE,
                    CassandraKeyValueServices.convertTtl(time, unit),
                    false);
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public void multiPut(
            Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable,
            long timestamp,
            long time,
            TimeUnit unit) throws KeyAlreadyExistsException {
        Map<ResultSetFuture, TableReference> resultSetFutures = Maps.newHashMap();
        for (Entry<TableReference, ? extends Map<Cell, byte[]>> e : valuesByTable.entrySet()) {
            final TableReference table = e.getKey();
            // We sort here because some key value stores are more efficient if you store adjacent keys together.
            NavigableMap<Cell, byte[]> sortedMap = ImmutableSortedMap.copyOf(e.getValue());


            Iterable<List<Entry<Cell, byte[]>>> partitions = partitionByCountAndBytes(
                    sortedMap.entrySet(),
                    getMultiPutBatchCount(),
                    getMultiPutBatchSizeBytes(),
                    table,
                    CqlKeyValueServices.MULTIPUT_ENTRY_SIZING_FUNCTION);


            for (final List<Entry<Cell, byte[]>> p : partitions) {
                List<Entry<Cell, Value>> partition = Lists.transform(p,
                        input -> Maps.immutableEntry(input.getKey(), Value.create(input.getValue(), timestamp)));
                resultSetFutures.put(getPutPartitionResultSetFuture(
                        table,
                        partition,
                        TransactionType.NONE,
                        CassandraKeyValueServices.convertTtl(time, unit)), table);
            }
        }

        for (Entry<ResultSetFuture, TableReference> result : resultSetFutures.entrySet()) {
            ResultSet resultSet;
            try {
                resultSet = result.getKey().getUninterruptibly();
                resultSet.all();
            } catch (Throwable t) {
                throw Throwables.throwUncheckedException(t);
            }
            cqlKeyValueServices.logTracedQuery(getPutQuery(
                    result.getValue(),
                    CassandraKeyValueServices.convertTtl(time, unit)),
                    resultSet,
                    session,
                    cqlStatementCache.normalQuery);
        }
    }

}

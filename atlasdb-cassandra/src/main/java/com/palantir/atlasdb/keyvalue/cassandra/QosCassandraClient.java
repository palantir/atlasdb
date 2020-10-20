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

package com.palantir.atlasdb.keyvalue.cassandra;

import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.Query;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.QueryWeight;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.ThriftQueryWeighers;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.ThriftQueryWeighers.QueryWeigher;
import com.palantir.atlasdb.qos.metrics.QosMetrics;
import com.palantir.atlasdb.util.MetricsManager;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"all"}) // thrift variable names.
public class QosCassandraClient implements AutoDelegate_CassandraClient {
    private static final Logger log = LoggerFactory.getLogger(QosCassandraClient.class);

    private final CassandraClient client;
    private final QosMetrics metrics;
    private final Ticker ticker;

    public QosCassandraClient(CassandraClient client, QosMetrics metrics, Ticker ticker) {
        this.client = client;
        this.metrics = metrics;
        this.ticker = ticker;
    }

    public static QosCassandraClient instrumentWithMetrics(CassandraClient client, MetricsManager manager) {
        return new QosCassandraClient(client, new QosMetrics(manager), Ticker.systemTicker());
    }

    @Override
    public CassandraClient delegate() {
        return this.client;
    }

    @Override
    public Map<ByteBuffer, List<ColumnOrSuperColumn>> multiget_slice(
            String kvsMethodName,
            TableReference tableRef,
            List<ByteBuffer> keys,
            SlicePredicate predicate,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        return executeRead(
                () -> client.multiget_slice(kvsMethodName, tableRef, keys, predicate, consistency_level),
                ThriftQueryWeighers.multigetSlice(keys));
    }

    @Override
    public List<KeySlice> get_range_slices(
            String kvsMethodName,
            TableReference tableRef,
            SlicePredicate predicate,
            KeyRange range,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        return executeRead(
                () -> client.get_range_slices(kvsMethodName, tableRef, predicate, range, consistency_level),
                ThriftQueryWeighers.getRangeSlices(range));
    }

    @Override
    public void remove(
            String kvsMethodName,
            TableReference tableRef,
            byte[] row,
            long timestamp,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        executeWrite(
                () -> {
                    client.remove(kvsMethodName, tableRef, row, timestamp, consistency_level);
                    return null;
                },
                ThriftQueryWeighers.remove(row));
    }

    @Override
    public void batch_mutate(
            String kvsMethodName,
            Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        executeWrite(
                () -> {
                    client.batch_mutate(kvsMethodName, mutation_map, consistency_level);
                    return null;
                },
                ThriftQueryWeighers.batchMutate(mutation_map));
    }

    @Override
    public ColumnOrSuperColumn get(
            TableReference tableReference, ByteBuffer key, byte[] column, ConsistencyLevel consistency_level)
            throws InvalidRequestException, NotFoundException, UnavailableException, TimedOutException, TException {
        return executeRead(() -> client.get(tableReference, key, column, consistency_level), ThriftQueryWeighers.GET);
    }

    @Override
    public CqlResult execute_cql3_query(CqlQuery cqlQuery, Compression compression, ConsistencyLevel consistency)
            throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException,
                    TException {
        return executeRead(
                () -> client.execute_cql3_query(cqlQuery, compression, consistency),
                ThriftQueryWeighers.EXECUTE_CQL3_QUERY);
    }

    private <T, E extends Exception> T executeRead(Query<T, E> query, QueryWeigher<T> weigher) throws E {
        return execute(query, weigher, metrics::recordRead);
    }

    private <T, E extends Exception> T executeWrite(Query<T, E> query, QueryWeigher<T> weigher) throws E {
        return execute(query, weigher, metrics::recordWrite);
    }

    private <T, E extends Exception> T execute(
            Query<T, E> query, QueryWeigher<T> weigher, Consumer<QueryWeight> weightMetric) throws E {
        Stopwatch timer = Stopwatch.createStarted(ticker);

        QueryWeight actualWeight = null;
        try {
            T result = query.execute();
            actualWeight = weigher.weighSuccess(result, timer.elapsed(TimeUnit.NANOSECONDS));
            return result;
        } catch (Exception ex) {
            actualWeight = weigher.weighFailure(ex, timer.elapsed(TimeUnit.NANOSECONDS));
            throw ex;
        } finally {
            weightMetric.accept(actualWeight);
        }
    }
}

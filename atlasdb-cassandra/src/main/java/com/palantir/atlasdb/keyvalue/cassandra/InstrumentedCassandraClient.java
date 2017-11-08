/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.thrift.AutoDelegate_Client;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.palantir.atlasdb.tracing.CloseableTrace;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.processors.AutoDelegate;

@AutoDelegate(typeToExtend = Cassandra.Client.class)
public class InstrumentedCassandraClient extends AutoDelegate_Client {
    private static final String SERVICE_NAME = "cassandra-thrift-client";
    private static final MetricRegistry METRIC_REGISTRY = AtlasDbMetrics.getMetricRegistry();

    private static final Timer MULTIGET_SLICE_TIMER = METRIC_REGISTRY.timer("cassandra.multiget_slice");
    private static final Timer BATCH_MUTATE_TIMER = METRIC_REGISTRY.timer("cassandra.batch_mutate");
    private static final Timer GET_RANGE_SLICE_TIMER = METRIC_REGISTRY.timer("cassandra.get_range_slice");

    private Cassandra.Client delegate;

    public InstrumentedCassandraClient(Cassandra.Client delegate) {
        super(delegate.getInputProtocol());
        this.delegate = delegate;
    }

    @Override
    public Cassandra.Client delegate() {
        return delegate;
    }

    @Override
    public void batch_mutate(Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("client.batch_mutate(number of rows {}, consistency {})",
                mutation_map.size(), toString(consistency_level))) {
            registerDuration(() -> {
                delegate.batch_mutate(mutation_map, consistency_level);
                return null;
            }, BATCH_MUTATE_TIMER);
        }
    }

    @Override
    public Map<ByteBuffer, List<ColumnOrSuperColumn>> multiget_slice(List<ByteBuffer> keys, ColumnParent column_parent,
            SlicePredicate predicate, ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace(
                "client.multiget_slice(number of keys {}, number of columns {}, consistency {})",
                keys.size(), predicate.slice_range.count, toString(consistency_level))) {
            return registerDuration(
                    () -> delegate.multiget_slice(keys, column_parent, predicate, consistency_level),
                    MULTIGET_SLICE_TIMER);
        }
    }

    @Override
    public List<KeySlice> get_range_slices(ColumnParent column_parent, SlicePredicate predicate, KeyRange range,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace(
                "client.get_range_slices(number of keys {}, number of columns {}, consistency {})",
                predicate.slice_range.count, range.count, toString(consistency_level))) {
            return registerDuration(
                    () -> delegate.get_range_slices(column_parent, predicate, range, consistency_level),
                    GET_RANGE_SLICE_TIMER);
        }
    }

    private <T> T registerDuration(CallableTException<T> callable, Timer timer) throws TException {
        long startTime = System.nanoTime();
        T ret = callable.call();
        long endTime = System.nanoTime();
        timer.update(endTime - startTime, TimeUnit.NANOSECONDS);

        return ret;
    }

    private static CloseableTrace startLocalTrace(CharSequence operationFormat, Object... formatArguments) {
        return CloseableTrace.startLocalTrace(SERVICE_NAME, operationFormat, formatArguments);
    }

    private static String toString(ConsistencyLevel consistency) {
        switch (consistency) {
            case ONE:
                return "one";
            case QUORUM:
                return "quorum";
            case LOCAL_QUORUM:
                return "local_quorum";
            case EACH_QUORUM:
                return "each_quorum";
            case ALL:
                return "all";
            case ANY:
                return "any";
            case TWO:
                return "two";
            case THREE:
                return "three";
            case SERIAL:
                return "serial";
            case LOCAL_SERIAL:
                return "local_serial";
            case LOCAL_ONE:
                return "local_one";
        }

        return "unknown";
    }

    private interface CallableTException<T> extends Callable<T> {
        T call() throws TException;
    }
}

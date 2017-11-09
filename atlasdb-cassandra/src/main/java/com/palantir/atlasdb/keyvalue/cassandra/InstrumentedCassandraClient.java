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
import java.util.function.Consumer;

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
import org.slf4j.Logger;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.palantir.atlasdb.logging.KvsProfilingLogger;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.tracing.CloseableTrace;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.logsafe.SafeArg;
import com.palantir.processors.AutoDelegate;

@AutoDelegate(typeToExtend = Cassandra.Client.class)
@SuppressWarnings({"all"}) // thrift variable names
public class InstrumentedCassandraClient extends AutoDelegate_Client {
    private static final String SERVICE_NAME = "cassandra-thrift-client";
    private static final MetricRegistry METRIC_REGISTRY = AtlasDbMetrics.getMetricRegistry();

    private static final Timer MULTIGET_SLICE_TIMER = METRIC_REGISTRY.timer(
            MetricRegistry.name(InstrumentedCassandraClient.class, "multiget_slice"));
    private static final Timer BATCH_MUTATE_TIMER = METRIC_REGISTRY.timer(
            MetricRegistry.name(InstrumentedCassandraClient.class, "batch_mutate"));
    private static final Timer GET_RANGE_SLICE_TIMER = METRIC_REGISTRY.timer(
            MetricRegistry.name(InstrumentedCassandraClient.class, "get_range"));

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
        // TODO(ssouza): also log the row key names when they can be marked as safe for logging.
        int numberOfMutations = mutation_map.size();

        try (CloseableTrace trace = startLocalTrace("client.batch_mutate(number of mutations {}, consistency {})",
                numberOfMutations, consistency_level)) {
            registerDuration(() -> {
                        delegate.batch_mutate(mutation_map, consistency_level);
                        return null;
                    }, BATCH_MUTATE_TIMER,
                    logger -> logger.warn("client.batch_mutate(number of rows {}, consistency {}",
                            SafeArg.of("number of mutations", numberOfMutations),
                            SafeArg.of("consistency", consistency_level.toString())));
        }
    }

    @Override
    public Map<ByteBuffer, List<ColumnOrSuperColumn>> multiget_slice(List<ByteBuffer> keys, ColumnParent column_parent,
            SlicePredicate predicate, ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        //noinspection unused - try-with-resources closes trace
        // TODO(ssouza): also log the row key names when they can be marked as safe for logging.
        String internalTableReference = column_parent.column_family;
        int numberOfKeys = keys.size();
        int numberOfColumns = predicate.slice_range.count;

        try (CloseableTrace trace = startLocalTrace(
                "client.multiget_slice(table {}, number of keys {}, number of columns {}, consistency {})",
                LoggingArgs.safeInternalTableNameOrPlaceholder(internalTableReference),
                numberOfKeys, numberOfColumns, consistency_level)) {
            return registerDuration(
                    () -> delegate.multiget_slice(keys, column_parent, predicate, consistency_level),
                    MULTIGET_SLICE_TIMER,
                    logger -> logger.warn("client.multiget_slice({}, {}, {}, {})",
                            LoggingArgs.safeInternalTableNameOrPlaceholder(internalTableReference),
                            SafeArg.of("number of keys", numberOfKeys),
                            SafeArg.of("number of columns", numberOfColumns),
                            SafeArg.of("consistency", consistency_level.toString())));
        }
    }

    @Override
    public List<KeySlice> get_range_slices(ColumnParent column_parent, SlicePredicate predicate, KeyRange range,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        //noinspection unused - try-with-resources closes trace
        // TODO(ssouza): also log the row key names when they can be marked as safe for logging.
        String internalTableRef = column_parent.column_family;
        int numberOfKeys = predicate.slice_range.count;
        int numberOfColumns = range.count;

        try (CloseableTrace trace = startLocalTrace(
                "client.get_range_slices(table {}, number of keys {}, number of columns {}, consistency {})",
                LoggingArgs.safeInternalTableNameOrPlaceholder(internalTableRef),
                numberOfKeys, numberOfColumns, consistency_level)) {
            return registerDuration(
                    () -> delegate.get_range_slices(column_parent, predicate, range, consistency_level),
                    GET_RANGE_SLICE_TIMER,
                    logger -> logger.warn("client.get_range_slices({}, {}, {}, {}",
                            LoggingArgs.safeInternalTableNameOrPlaceholder(internalTableRef),
                            SafeArg.of("number of keys", numberOfKeys),
                            SafeArg.of("number of columns", numberOfColumns),
                            SafeArg.of("consistency", consistency_level.toString())));
        }
    }

    private <T> T registerDuration(CallableTException<T> callable, Timer timer, Consumer<Logger> loggerFunction)
            throws TException {

        long startTime = System.nanoTime();
        T ret = callable.call();
        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        timer.update(duration, TimeUnit.NANOSECONDS);

        long slowLogThreshold = TimeUnit.MILLISECONDS.toNanos(KvsProfilingLogger.DEFAULT_THRESHOLD_MILLIS);
        Logger slowlogger = KvsProfilingLogger.slowlogger;
        if (duration > slowLogThreshold && slowlogger.isWarnEnabled()) {
            loggerFunction.accept(slowlogger);
        }

        return ret;
    }

    private static CloseableTrace startLocalTrace(CharSequence operationFormat, Object... formatArguments) {
        return CloseableTrace.startLocalTrace(SERVICE_NAME, operationFormat, formatArguments);
    }

    private interface CallableTException<T> extends Callable<T> {
        T call() throws TException;
    }
}

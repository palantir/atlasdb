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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.slf4j.Logger;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.sweep.CellWithTimestamp;
import com.palantir.atlasdb.logging.KvsProfilingLogger;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.tracing.CloseableTrace;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.logsafe.SafeArg;
import com.palantir.processors.AutoDelegate;

@AutoDelegate(typeToExtend = CqlExecutor.class)
public class InstrumentedCqlExecutor extends AutoDelegate_CqlExecutor {
    private static final String SERVICE_NAME = "cassandra-cql-executor";
    private static final MetricRegistry METRIC_REGISTRY = AtlasDbMetrics.getMetricRegistry();

    private static final Timer GET_TIMESTAMPS_TIMER = METRIC_REGISTRY.timer(
            MetricRegistry.name(CqlExecutor.class, "getTimestamps"));
    private static final Timer GET_TIMESTAMPS_WITHIN_ROW_TIMER = METRIC_REGISTRY.timer(
            MetricRegistry.name(CqlExecutor.class, "getTimestampsWithinRow"));

    private CqlExecutor delegate;

    public InstrumentedCqlExecutor(CqlExecutor delegate) {
        super(null, null);
        this.delegate = delegate;
    }

    @Override
    public CqlExecutor delegate() {
        return delegate;
    }

    @Override
    public List<CellWithTimestamp> getTimestamps(TableReference tableRef, byte[] startRowInclusive, int limit) {
        // TODO(ssouza): also log the row key names when they can be marked as safe for logging.

        // noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("cqlExecutor.getTimestamps(table {}, limit {})",
                LoggingArgs.safeTableOrPlaceholder(tableRef), limit)) {
            return registerDuration(() ->
                    delegate.getTimestamps(tableRef, startRowInclusive, limit), GET_TIMESTAMPS_TIMER,
                    logger -> logger.warn("cqlExecutor.getTimestamps({}, {})",
                            LoggingArgs.tableRef(tableRef),
                            SafeArg.of("limit", limit)));
        }
    }

    @Override
    public List<CellWithTimestamp> getTimestampsWithinRow(TableReference tableRef, byte[] row,
            byte[] startColumnInclusive, long startTimestampExclusive, int limit) {
        // TODO(ssouza): also log the row key names when they can be marked as safe for logging.

        //noinspection unused - try-with-resources closes trace
        try (CloseableTrace trace = startLocalTrace("cqlExecutor.getTimestampsWithinRow(table {}, ts {}, limit {})",
                LoggingArgs.safeTableOrPlaceholder(tableRef), startTimestampExclusive, limit)) {
            return registerDuration(() -> delegate.getTimestampsWithinRow(
                    tableRef, row, startColumnInclusive, startTimestampExclusive, limit),
                    GET_TIMESTAMPS_WITHIN_ROW_TIMER,
                    logger -> logger.warn("cqlExecutor.getTimestampsWithinRow({}, {}, {})",
                            LoggingArgs.tableRef(tableRef),
                            SafeArg.of("sweepTs", startTimestampExclusive),
                            SafeArg.of("limit", limit)));
        }
    }

    private <T> T registerDuration(Supplier<T> callable, Timer timer, Consumer<Logger> loggerFunction) {
        long startTime = System.nanoTime();
        T ret = callable.get();
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
}

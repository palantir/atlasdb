/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.async;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.annotation.concurrent.ThreadSafe;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.logsafe.Preconditions;


@org.immutables.value.Value.Immutable
public abstract class GetQuerySpec implements CqlQuerySpec<Optional<Value>> {

    /**
     * Since each query is constructed for one cell we are using an optimisation that we can ask the CQL to do most of
     * the work internally. First of all we are using the fact that timestamps in column2 are ordered in ASC order and
     * since we are interested in the most recent timestamp we use LIMIT 1 to get the latest value. This should help
     * with both cassandra workload and amount of transferred data.
     */
    private static final String QUERY_FORMAT = "SELECT value, column2 FROM \"%s\".\"%s\" "
            + "WHERE key = :row AND column1 = :column AND column2 > :timestamp "
            + "ORDER BY column1, column2 ASC "
            + "LIMIT 1;";

    @org.immutables.value.Value.Auxiliary
    public abstract ByteBuffer row();

    @org.immutables.value.Value.Auxiliary
    public abstract ByteBuffer column();

    @org.immutables.value.Value.Auxiliary
    public abstract long humanReadableTimestamp();

    public final Supplier<RowStreamAccumulator<Optional<Value>>> rowStreamAccumulatorFactory() {
        return GetQueryAccumulator::new;
    }

    private long queryTimestamp() {
        return ~humanReadableTimestamp();
    }

    public String formatQueryString() {
        return String.format(QUERY_FORMAT, keySpace(), AbstractKeyValueService.internalTableName(tableReference()));
    }

    @Override
    public BoundStatement bind(BoundStatement boundStatement) {
        return boundStatement.setBytes("row", row())
                .setBytes("column", column())
                .setLong("timestamp", queryTimestamp());
    }

    @ThreadSafe
    public static class GetQueryAccumulator implements RowStreamAccumulator<Optional<Value>> {

        private Optional<Value> resultValue = Optional.empty();

        @Override
        public synchronized void accumulateRowStream(Stream<Row> rowStream) {
            Preconditions.checkState(!resultValue.isPresent(),
                    "There should not be multiple calls to this method");
            resultValue = rowStream.map(GetQueryAccumulator::parseValue)
                    .max(Comparator.comparingLong(Value::getTimestamp));
        }

        @Override
        public synchronized Optional<Value> result() {
            return resultValue;
        }

        private static Value parseValue(Row row) {
            return Value.create(row.getBytes(0).array(), ~row.getLong(1));
        }
    }
}

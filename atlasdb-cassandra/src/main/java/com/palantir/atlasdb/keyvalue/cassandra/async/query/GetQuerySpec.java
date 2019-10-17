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

package com.palantir.atlasdb.keyvalue.cassandra.async.query;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.annotation.concurrent.ThreadSafe;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.async.RowStreamAccumulator;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.logsafe.Preconditions;


@org.immutables.value.Value.Immutable
public abstract class GetQuerySpec implements CqlQuerySpec<Optional<Value>> {

    /**
     * Since each query is constructed for one cell we are using an optimisation that we can ask the CQL to do most of
     * the work internally. First of all we are using the fact that timestamps in {@code column2} are ordered in
     * {@code ASC} order and since we are interested in the most recent timestamp we use {@code LIMIT 1} to get the
     * latest value. This should help with both cassandra workload and amount of transferred data.
     */
    private static final String QUERY_FORMAT = "SELECT value, column2 FROM \"%s\".\"%s\" "
            + "WHERE key = :row AND column1 = :column AND column2 > :timestamp "
            + "ORDER BY column1, column2 ASC "
            + "LIMIT 1;";

    @Override
    public String formatQueryString() {
        return String.format(
                QUERY_FORMAT,
                cqlQueryContext().keySpace(),
                AbstractKeyValueService.internalTableName(cqlQueryContext().tableReference()));
    }

    @Override
    public Statement makeExecutableStatement(PreparedStatement preparedStatement) {
        return preparedStatement.bind()
                .setBytes("row", getQueryParameters().row())
                .setBytes("column", getQueryParameters().column())
                .setLong("timestamp", getQueryParameters().queryTimestamp());
    }

    @Override
    public ConsistencyLevel queryConsistency() {
        return ConsistencyLevel.LOCAL_QUORUM;
    }

    @Override
    public Supplier<RowStreamAccumulator<Optional<Value>>> rowStreamAccumulatorFactory() {
        return GetQueryAccumulator::new;
    }

    @org.immutables.value.Value.Auxiliary
    public abstract GetQueryParameters getQueryParameters();

    @org.immutables.value.Value.Immutable
    public interface GetQueryParameters {
        ByteBuffer row();

        ByteBuffer column();

        long humanReadableTimestamp();

        default long queryTimestamp() {
            return ~humanReadableTimestamp();
        }
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

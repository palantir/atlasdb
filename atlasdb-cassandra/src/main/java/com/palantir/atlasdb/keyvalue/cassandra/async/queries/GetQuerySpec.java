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

package com.palantir.atlasdb.keyvalue.cassandra.async.queries;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.logsafe.Preconditions;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

public final class GetQuerySpec implements CqlQuerySpec<Optional<Value>> {

    /**
     * Since each query is constructed for one cell we are using an optimisation that we can ask the CQL to do most of
     * the work internally. First of all we are using the fact that cells are clustered in ASC ordered by
     * {@code column1/column} and {@code column2/timestamp} value and since we are interested in the freshest value
     * before a certain timestamp we use {@code LIMIT 1} to get the latest value. This helps with both cassandra
     * workload and amount of transferred data. Timestamps are stored as bitwise complements of the original values.
     */
    private static final String QUERY_FORMAT = "SELECT value, column2 FROM \"%s\".\"%s\" "
            + "WHERE key = :row AND column1 = :column AND column2 > :timestamp "
            + "LIMIT 1;";

    private final CqlQueryContext cqlQueryContext;
    private final GetQueryParameters getQueryParameters;
    private final GetQueryAccumulator getQueryAccumulator = new GetQueryAccumulator();

    public GetQuerySpec(CqlQueryContext cqlQueryContext, GetQueryParameters getQueryParameters) {
        this.cqlQueryContext = cqlQueryContext;
        this.getQueryParameters = getQueryParameters;
    }

    @Override
    public CqlQueryContext cqlQueryContext() {
        return cqlQueryContext;
    }

    @Override
    public String formatQueryString() {
        return String.format(
                QUERY_FORMAT,
                cqlQueryContext().keyspace(),
                AbstractKeyValueService.internalTableName(cqlQueryContext().tableReference()));
    }

    @Override
    public QueryType queryType() {
        return QueryType.GET;
    }

    @Override
    public Statement makeExecutableStatement(PreparedStatement preparedStatement) {
        return preparedStatement
                .bind()
                .setBytes("row", toReadOnlyByteBuffer(getQueryParameters.cell().getRowName()))
                .setBytes(
                        "column", toReadOnlyByteBuffer(getQueryParameters.cell().getColumnName()))
                .setLong("timestamp", getQueryParameters.queryTimestamp());
    }

    private static ByteBuffer toReadOnlyByteBuffer(byte[] bytes) {
        return ByteBuffer.wrap(bytes).asReadOnlyBuffer();
    }

    @Override
    public ConsistencyLevel queryConsistency() {
        return ConsistencyLevel.LOCAL_QUORUM;
    }

    @Override
    public RowStreamAccumulator<Optional<Value>> rowStreamAccumulator() {
        return getQueryAccumulator;
    }

    @org.immutables.value.Value.Immutable
    public interface GetQueryParameters {
        Cell cell();

        long humanReadableTimestamp();

        default long queryTimestamp() {
            return ~humanReadableTimestamp();
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        GetQuerySpec that = (GetQuerySpec) other;
        return cqlQueryContext.equals(that.cqlQueryContext) && getQueryParameters.equals(that.getQueryParameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cqlQueryContext, getQueryParameters);
    }

    private static final class GetQueryAccumulator implements RowStreamAccumulator<Optional<Value>> {

        private volatile Value resultValue = null;
        private volatile boolean assigned = false;

        @Override
        public void accumulateRowStream(Stream<Row> rowStream) {
            Preconditions.checkState(
                    !assigned, "Multiple calls to accumulateRowStream, wrong usage of this implementation");
            // the query can only ever return one page with one row
            assigned = true;
            resultValue =
                    rowStream.findFirst().map(GetQueryAccumulator::parseValue).orElse(null);
        }

        @Override
        public Optional<Value> result() {
            Preconditions.checkState(assigned, "Result has never been assigned(query associated never ran).");
            return Optional.ofNullable(resultValue);
        }

        private static Value parseValue(Row row) {
            return Value.create(row.getBytes(0).array(), ~row.getLong(1));
        }
    }
}

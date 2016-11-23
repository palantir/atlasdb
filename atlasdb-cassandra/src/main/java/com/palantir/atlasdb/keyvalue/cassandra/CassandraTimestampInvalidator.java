/**
 * Copyright 2016 Palantir Technologies
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

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Mutation;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.common.base.Throwables;
import com.palantir.timestamp.TimestampInvalidator;

public class CassandraTimestampInvalidator implements TimestampInvalidator {
    private static final Logger log = LoggerFactory.getLogger(CassandraTimestampInvalidator.class);
    private static final long CASSANDRA_TIMESTAMP = 0L;
    private static final String ROW_AND_COLUMN_NAME = "ts";

    public static final TableMetadata BOGUS_TIMESTAMP_TABLE_METADATA = new TableMetadata(
            NameMetadataDescription.create(ImmutableList.of(
                    new NameComponentDescription("timestamp_name", ValueType.STRING))),
            new ColumnMetadataDescription(ImmutableList.of(
                    new NamedColumnDescription(
                            ROW_AND_COLUMN_NAME,
                            "current_max_ts",
                            ColumnValueDescription.forType(ValueType.BLOB)))),
            ConflictHandler.IGNORE_ALL);

    private final CassandraKeyValueService rawKeyValueService;

    public CassandraTimestampInvalidator(CassandraKeyValueService rawKeyValueService) {
        this.rawKeyValueService = rawKeyValueService;
    }

    @Override
    public void invalidateTimestampTable() {
        rawKeyValueService.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        rawKeyValueService.createTable(AtlasDbConstants.TIMESTAMP_TABLE,
                BOGUS_TIMESTAMP_TABLE_METADATA.persistToBytes());
        rawKeyValueService.clientPool.runWithRetry(
                input -> {
                    try {
                        Mutation mutation = makeBogusMutation();
                        Map<String, List<Mutation>> cfValues =
                                ImmutableMap.of(AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName(), ImmutableList.of(mutation));
                        input.batch_mutate(ImmutableMap.of(getRowName(), cfValues), ConsistencyLevel.QUORUM);
                        return null;
                    } catch (TException e) {
                        log.error("Error trying to install bogus column.");
                        throw Throwables.rewrapAndThrowUncheckedException(e);
                    }
                }
        );
    }

    private Mutation makeBogusMutation() {
        Mutation mutation = new Mutation();
        mutation.setColumn_or_supercolumn(
                new ColumnOrSuperColumn().setColumn(makeBogusColumn()));
        return mutation;
    }

    private Column makeBogusColumn() {
        Column col = new Column();
        col.setName(getColumnName());
        col.setValue(new byte[0]);
        col.setTimestamp(CASSANDRA_TIMESTAMP);
        return col;
    }

    private static byte[] getColumnName() {
        return CassandraKeyValueServices
                .makeCompositeBuffer(PtBytes.toBytes(ROW_AND_COLUMN_NAME), CASSANDRA_TIMESTAMP)
                .array();
    }

    private static ByteBuffer getRowName() {
        return ByteBuffer.wrap(PtBytes.toBytes(ROW_AND_COLUMN_NAME));
    }
}

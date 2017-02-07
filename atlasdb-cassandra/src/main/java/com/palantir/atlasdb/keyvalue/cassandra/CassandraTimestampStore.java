/**
 * Copyright 2017 Palantir Technologies
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
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Optional;

import javax.xml.bind.DatatypeConverter;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
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
import com.palantir.timestamp.DebugLogger;
import com.palantir.util.debug.ThreadDumps;

public class CassandraTimestampStore {
    private static final Logger log = LoggerFactory.getLogger(CassandraTimestampBoundStore.class);

    private static final long CASSANDRA_TIMESTAMP = 0L;
    private static final String ROW_AND_COLUMN_NAME = "ts";
    private static final String BACKUP_COLUMN_NAME = "oldTs";
    private static final byte[] INVALIDATED_VALUE = new byte[1];

    private static final byte[] SUCCESSFUL_OPERATION = {1};

    public static final TableMetadata TIMESTAMP_TABLE_METADATA = new TableMetadata(
            NameMetadataDescription.create(ImmutableList.of(
                    new NameComponentDescription("timestamp_name", ValueType.STRING))),
            new ColumnMetadataDescription(ImmutableList.of(
                    new NamedColumnDescription(
                            ROW_AND_COLUMN_NAME,
                            "current_max_ts",
                            ColumnValueDescription.forType(ValueType.FIXED_LONG)))),
            ConflictHandler.IGNORE_ALL);

    private final CassandraKeyValueService cassandraKeyValueService;

    public CassandraTimestampStore(CassandraKeyValueService cassandraKeyValueService) {
        this.cassandraKeyValueService = cassandraKeyValueService;
    }

    /**
     * Creates the timestamp table. Note that this operation is idempotent.
     */
    public void createTimestampTable() {
        cassandraKeyValueService.createTable(
                AtlasDbConstants.TIMESTAMP_TABLE,
                TIMESTAMP_TABLE_METADATA.persistToBytes());
    }

    /**
     * Gets the upper timestamp limit from the database if it exists.
     * @return Timestamp limit stored in the KVS
     */
    public synchronized Optional<Long> getUpperLimit() {
        return cassandraKeyValueService.clientPool.runWithRetry(client -> {
            String selectQuery = String.format(
                    "SELECT value FROM %s WHERE key=%s AND column1=%s;",
                    wrapInQuotes(AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName()),
                    encodeCassandraHexString(ROW_AND_COLUMN_NAME),
                    encodeCassandraHexString(ROW_AND_COLUMN_NAME));
            ByteBuffer queryBuffer = ByteBuffer.wrap(PtBytes.toBytes(selectQuery));
            try {
                CqlResult result = client.execute_cql3_query(queryBuffer, Compression.NONE, ConsistencyLevel.QUORUM);
                List<CqlRow> cqlRows = result.getRows();
                if (cqlRows.isEmpty()) {
                    return Optional.empty();
                }
                return Optional.of(PtBytes.toLong(Iterables.getOnlyElement(getColumnsFromOnlyRow(cqlRows)).getValue()));
            } catch (TException e) {
                throw Throwables.rewrapAndThrowUncheckedException(e);
            }
        });
    }

    public synchronized void storeTimestampBound(Long expected, long target) {
        cassandraKeyValueService.clientPool.runWithRetry(client -> {
            String updateQuery;
            if (expected == null) {
                updateQuery = String.format(
                        "INSERT INTO %s (key, column1, column2, value) VALUES (%s, %s, -1, %s) IF NOT EXISTS;",
                        wrapInQuotes(AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName()),
                        encodeCassandraHexString(ROW_AND_COLUMN_NAME),
                        encodeCassandraHexString(ROW_AND_COLUMN_NAME),
                        encodeCassandraHexBytes(PtBytes.toBytes(target)));
            } else {
                updateQuery = String.format(
                        "UPDATE %s SET value=%s WHERE key=%s AND column1=%s AND column2=-1 IF value=%s;",
                        wrapInQuotes(AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName()),
                        encodeCassandraHexBytes(PtBytes.toBytes(target)),
                        encodeCassandraHexString(ROW_AND_COLUMN_NAME),
                        encodeCassandraHexString(ROW_AND_COLUMN_NAME),
                        encodeCassandraHexBytes(PtBytes.toBytes(expected)));
            }
            ByteBuffer queryBuffer = ByteBuffer.wrap(PtBytes.toBytes(updateQuery));
            try {
                CqlResult result = client.execute_cql3_query(queryBuffer, Compression.NONE, ConsistencyLevel.QUORUM);
                Column appliedColumn = getColumnsFromOnlyRow(result.getRows()).get(0);
                if (!Arrays.equals(appliedColumn.getValue(), SUCCESSFUL_OPERATION)) {
                    throw constructConcurrentTimestampStoreException(expected, target, result);
                }
                return null;
            } catch (TException e) {
                throw Throwables.rewrapAndThrowUncheckedException(e);
            }
        });
    }

    private ConcurrentModificationException constructConcurrentTimestampStoreException(
            Long expected,
            long target,
            CqlResult result) {
        Column valueColumn;
        if (expected == null) {
            valueColumn = getColumnsFromOnlyRow(result.getRows()).get(4);
        } else {
            valueColumn = getColumnsFromOnlyRow(result.getRows()).get(1);
        }
        long actual = PtBytes.toLong(valueColumn.getValue());

        String msg = "Unable to CAS from {} to {}."
                + " Timestamp limit changed underneath us (limit in memory: {}, stored in DB: {}).";
        ConcurrentModificationException except = new ConcurrentModificationException(
                String.format(replaceBracesWithStringFormatSpecifier(msg),
                        expected,
                        target,
                        expected,
                        actual));
        log.error(msg, expected, target, expected, actual);
        DebugLogger.logger.error("Thread dump: {}", ThreadDumps.programmaticThreadDump());
        throw except;
    }

    /**
     * Stores a backup of the existing timestamp.
     */
    public synchronized void backupExistingTimestamp() {
        throw new UnsupportedOperationException("TODO implement me");
    }

    public synchronized void restoreFromBackup() {
        throw new UnsupportedOperationException("TODO implement me");
    }

    private static String wrapInQuotes(String tableName) {
        return "\"" + tableName + "\"";
    }

    private static String encodeCassandraHexString(String string) {
        return encodeCassandraHexBytes(PtBytes.toBytes(string));
    }

    private static String encodeCassandraHexBytes(byte[] bytes) {
        return "0x" + DatatypeConverter.printHexBinary(bytes);
    }

    private static List<Column> getColumnsFromOnlyRow(List<CqlRow> cqlRows) {
        return Iterables.getOnlyElement(cqlRows).getColumns();
    }

    private static String replaceBracesWithStringFormatSpecifier(String msg) {
        return msg.replaceAll("\\{\\}", "%s");
    }
}

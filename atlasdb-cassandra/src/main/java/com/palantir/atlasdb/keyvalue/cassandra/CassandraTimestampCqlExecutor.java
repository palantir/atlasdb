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
import java.util.Map;
import java.util.stream.Collectors;

import javax.xml.bind.DatatypeConverter;

import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.common.base.Throwables;

public class CassandraTimestampCqlExecutor {
    private static final Logger log = LoggerFactory.getLogger(CassandraTimestampCqlExecutor.class);

    private static final String ROW_AND_COLUMN_NAME = "ts";
    private static final String BACKUP_COLUMN_NAME = "oldTs";

    static final byte[] ROW_AND_COLUMN_NAME_BYTES = PtBytes.toBytes(ROW_AND_COLUMN_NAME);
    static final byte[] BACKUP_COLUMN_NAME_BYTES = PtBytes.toBytes(BACKUP_COLUMN_NAME);

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final CassandraKeyValueService rawCassandraKvs;

    public CassandraTimestampCqlExecutor(CassandraKeyValueService rawCassandraKvs) {
        this.rawCassandraKvs = rawCassandraKvs;
    }

    public void backupBound(long bound) {
        executeCqlBatchInsertOnTimestampTable(ImmutableMap.<byte[], byte[]>builder()
                .put(ROW_AND_COLUMN_NAME_BYTES, EMPTY_BYTE_ARRAY)
                .put(BACKUP_COLUMN_NAME_BYTES, PtBytes.toBytes(bound))
                .build());
    }

    public void restoreBoundFromBackup(long bound) {
        executeCqlBatchInsertOnTimestampTable(ImmutableMap.<byte[], byte[]>builder()
                .put(ROW_AND_COLUMN_NAME_BYTES, PtBytes.toBytes(bound))
                .put(BACKUP_COLUMN_NAME_BYTES, EMPTY_BYTE_ARRAY) // else 2x revalidate will mean we go back in time
                .build());
    }

    private void executeCqlBatchInsertOnTimestampTable(Map<byte[], byte[]> rowsToValues) {
        rawCassandraKvs.clientPool.runWithRetry(client -> {
            try {
                String queryString = constructCqlBatchInsertCommand(rowsToValues);
                ByteBuffer queryBuffer = ByteBuffer.wrap(PtBytes.toBytes(queryString));
                client.execute_cql3_query(queryBuffer, Compression.NONE, ConsistencyLevel.QUORUM);
            } catch (TException e) {
                log.error("An error occurred whilst trying to write to Cassandra:", e);
                throw Throwables.rewrapAndThrowUncheckedException(e);
            }
            return null;
        });
    }

    private String constructCqlBatchInsertCommand(Map<byte[], byte[]> rowsToValues) {
        String insertions = rowsToValues.entrySet()
                .stream()
                .map(entry -> constructCqlInsertCommandForTimestampTable(entry.getKey(), entry.getValue()))
                .collect(Collectors.joining());
        return "BEGIN BATCH\n"
                + insertions
                + "APPLY BATCH;";
    }

    private String constructCqlInsertCommandForTimestampTable(byte[] rowAndColumnName, byte[] value) {
        String hexName = encodeCassandraHexValue(rowAndColumnName);
        String hexValue = encodeCassandraHexValue(value);
        return String.format(
                "INSERT INTO %s (key, column1, column2, value) VALUES (%s, %s, -1, %s);%n",
                wrapInQuotes(AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName()),
                hexName,
                hexName,
                hexValue);
    }

    private String wrapInQuotes(String tableName) {
        return "\"" + tableName + "\"";
    }

    private String encodeCassandraHexValue(byte[] value) {
        return "0x" + DatatypeConverter.printHexBinary(value);
    }
}

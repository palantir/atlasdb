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
import java.util.Optional;

import javax.xml.bind.DatatypeConverter;

import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.Throwables;
import com.palantir.timestamp.TimestampStoreInvalidator;

public class CassandraTimestampStoreInvalidator implements TimestampStoreInvalidator {
    private static final Logger log = LoggerFactory.getLogger(CassandraTimestampStoreInvalidator.class);
    private static final long CASSANDRA_TIMESTAMP = 0L;

    private static final String ROW_AND_COLUMN_NAME = "ts";
    private static final byte[] ROW_AND_COLUMN_NAME_BYTES = PtBytes.toBytes(ROW_AND_COLUMN_NAME);
    private static final Cell TIMESTAMP_CELL = Cell.create(ROW_AND_COLUMN_NAME_BYTES, ROW_AND_COLUMN_NAME_BYTES);

    private static final String BACKUP_COLUMN_NAME = "oldTs";
    private static final byte[] BACKUP_COLUMN_NAME_BYTES = PtBytes.toBytes(BACKUP_COLUMN_NAME);

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final CassandraKeyValueService rawCassandraKvs;

    public CassandraTimestampStoreInvalidator(CassandraKeyValueService rawCassandraKvs) {
        this.rawCassandraKvs = rawCassandraKvs;
    }

    @Override
    public void invalidateTimestampStore() {
        Optional<Long> bound = getCurrentTimestampBound();
        bound.ifPresent(this::backupBound);
    }

    private Optional<Long> getCurrentTimestampBound() {
        Map<Cell, Value> result = rawCassandraKvs.get(
                AtlasDbConstants.TIMESTAMP_TABLE,
                ImmutableMap.of(TIMESTAMP_CELL, Long.MAX_VALUE));
        Value value = Iterables.getOnlyElement(result.values(), Value.create(PtBytes.toBytes(0L), CASSANDRA_TIMESTAMP));
        try {
            return Optional.of(PtBytes.toLong(value.getContents()));
        } catch (IllegalArgumentException exception) {
            // This can occur if the timestamp table was already invalidated.
            return Optional.empty();
        }
    }

    private void backupBound(long bound) {
        rawCassandraKvs.clientPool.runWithRetry(client -> {
            try {
                String queryString = "BEGIN BATCH\n" +
                        constructCqlInsertCommandForTimestampTable(BACKUP_COLUMN_NAME_BYTES, PtBytes.toBytes(bound)) +
                        constructCqlInsertCommandForTimestampTable(ROW_AND_COLUMN_NAME_BYTES, EMPTY_BYTE_ARRAY) +
                        "APPLY BATCH;";
                ByteBuffer query = ByteBuffer.wrap(PtBytes.toBytes(queryString));
                client.execute_cql3_query(query, Compression.NONE, ConsistencyLevel.QUORUM);
            } catch (TException e) {
                log.error("An error occurred whilst trying to write to Cassandra:", e);
                throw Throwables.rewrapAndThrowUncheckedException(e);
            }
            return null;
        });
    }

    private String constructCqlInsertCommandForTimestampTable(byte[] rowAndColumnName, byte[] value) {
        String hexName = encodeCassandraHexValue(rowAndColumnName);
        String hexValue = encodeCassandraHexValue(value);
        return String.format(
                "INSERT INTO %s (key, column1, column2, value) VALUES (%s, %s, -1, %s);\n",
                "\"" + AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName() + "\"",
                hexName,
                hexName,
                hexValue);
    }

    private String encodeCassandraHexValue(byte[] value) {
        return "0x" + DatatypeConverter.printHexBinary(value);
    }

    @Override
    public void revalidateTimestampStore() {

    }
}

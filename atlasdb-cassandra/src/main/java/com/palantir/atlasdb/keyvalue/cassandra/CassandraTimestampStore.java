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
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.Optional;

import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.common.base.Throwables;
import com.palantir.timestamp.DebugLogger;
import com.palantir.util.Pair;
import com.palantir.util.debug.ThreadDumps;

public class CassandraTimestampStore {
    private static final Logger log = LoggerFactory.getLogger(CassandraTimestampStore.class);

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
                CassandraTimestampUtils.TIMESTAMP_TABLE_METADATA.persistToBytes());
    }

    /**
     * Gets the upper timestamp limit from the database if it exists.
     * @return Timestamp limit stored in the KVS, if present
     * @throws IllegalStateException if the timestamp limit has been invalidated
     */
    public synchronized Optional<Long> getUpperLimit() {
        return cassandraKeyValueService.clientPool.runWithRetry(client -> {
            ByteBuffer queryBuffer = CassandraTimestampUtils.constructSelectFromTimestampTableQuery();
            try {
                CqlResult result = client.execute_cql3_query(queryBuffer, Compression.NONE, ConsistencyLevel.QUORUM);
                Map<String, Long> columnarResults = CassandraTimestampUtils.getLongValuesFromSelectionResult(result);
                if (columnarResults.containsKey(CassandraTimestampUtils.ROW_AND_COLUMN_NAME)) {
                    return Optional.of(columnarResults.get(CassandraTimestampUtils.ROW_AND_COLUMN_NAME));
                }
                return Optional.empty();
            } catch (TException e) {
                throw Throwables.rewrapAndThrowUncheckedException(e);
            }
        });
    }

    /**
     * Stores an upper timestamp limit in the database in an atomic way.
     * @param expected Expected current value of the timestamp bound (or null if there is no bound)
     * @param target Upper timestamp limit we want to store in the database
     * @throws ConcurrentModificationException if the expected value does not match the bound in the database
     */
    public synchronized void storeTimestampBound(Long expected, long target) {
        cassandraKeyValueService.clientPool.runWithRetry(client -> {
            byte[] expectedBytes = getNullableByteArray(expected);
            byte[] targetBytes = PtBytes.toBytes(target);
            ByteBuffer queryBuffer = CassandraTimestampUtils.constructCheckAndSetMultipleQuery(
                    ImmutableMap.of(
                            CassandraTimestampUtils.ROW_AND_COLUMN_NAME,
                            Pair.create(expectedBytes, targetBytes)));
            try {
                CqlResult result = client.execute_cql3_query(queryBuffer, Compression.NONE, ConsistencyLevel.QUORUM);
                checkOperationWasApplied(expected, target, result);
                return null;
            } catch (TException e) {
                throw Throwables.rewrapAndThrowUncheckedException(e);
            }
        });
    }

    /**
     * Writes a backup of the existing timestamp to the database. After this backup, this timestamp service can
     * no longer be used until a restore.
     *
     * This may be implemented as follows:
     *  - Read the value of the backup timestamp. If this is readable, skip (already backed up).
     *  - Read the value of the timestamp (TS). If this is unreadable, fail.
     *  - Do a conditional logged batch:
     *    - CAS the main timestamp, expecting TS, to the INVALIDATED_VALUE
     *    - Put unless exists the value of TS to the backup timestamp.
     */
    public synchronized void backupExistingTimestamp() {
        throw new UnsupportedOperationException("TODO implement me");
    }

    /**
     * Restores a backup of an existing timestamp.
     *
     * This may be implemented following the inverse of the backup process:
     *  - Read the value of the timestamp. If this is readable, skip.
     *  - Read the value of the backup (BT). If this is unreadable, fail.
     *  - Do a conditional logged batch:
     *    - Conditionally delete the backup if it matches BT
     *    - CAS the main timestamp, expecting INVALIDATED_VALUE, to BT.
     */
    public synchronized void restoreFromBackup() {
        throw new UnsupportedOperationException("TODO implement me");
    }

    private void checkOperationWasApplied(Long expected, long target, CqlResult result) {
        if (!CassandraTimestampUtils.wasOperationApplied(result)) {
            throw constructConcurrentTimestampStoreException(expected, target, result);
        }
    }

    private static ConcurrentModificationException constructConcurrentTimestampStoreException(
            Long expected,
            long target,
            CqlResult result) {
        long actual = CassandraTimestampUtils.getLongValueFromApplicationResult(result);

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

    private static String replaceBracesWithStringFormatSpecifier(String msg) {
        return msg.replaceAll("\\{\\}", "%s");
    }

    private static byte[] getNullableByteArray(Long expected) {
        return expected == null ? null : PtBytes.toBytes(expected);
    }
}

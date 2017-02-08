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

import javax.annotation.Nullable;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.thrift.TException;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.common.annotation.Idempotent;
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
    @Idempotent
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
        return clientPool().runWithRetry(client -> {
            BoundData boundData = getCurrentBoundData(client);
            if (boundData.bound() == null) {
                return Optional.empty();
            }
            Preconditions.checkState(CassandraTimestampUtils.isValidTimestampData(boundData.bound()),
                    "The timestamp bound cannot be read.");
            return Optional.of(PtBytes.toLong(boundData.bound()));
        });
    }

    /**
     * Stores an upper timestamp limit in the database in an atomic way.
     * @param expected Expected current value of the timestamp bound (or null if there is no bound)
     * @param target Upper timestamp limit we want to store in the database
     * @throws ConcurrentModificationException if the expected value does not match the bound in the database
     */
    public synchronized void storeTimestampBound(Optional<Long> expected, long target) {
        clientPool().runWithRetry(client -> {
            byte[] expectedBytes = expected.map(PtBytes::toBytes).orElse(null);
            byte[] targetBytes = PtBytes.toBytes(target);
            ByteBuffer queryBuffer = CassandraTimestampUtils.constructCheckAndSetMultipleQuery(
                    ImmutableMap.of(
                            CassandraTimestampUtils.ROW_AND_COLUMN_NAME,
                            Pair.create(expectedBytes, targetBytes)));
            CqlResult result = executeQueryUnchecked(client, queryBuffer);
            checkOperationWasApplied(expected, target, result);
            return null;
        });
    }

    /**
     * Writes a backup of the existing timestamp to the database. After this backup, this timestamp service can
     * no longer be used until a restore.
     *
     * This may be implemented as follows:
     *  - Read the value of the backup timestamp. If this is readable, skip (already backed up).
     *  - Read the value of the timestamp (TS). If this is unreadable, fail.
     *  - Do a conditional unlogged batch (safe since the row key is the same)
     *    - CAS the main timestamp, expecting TS, to the INVALIDATED_VALUE
     *    - Put unless exists the value of TS to the backup timestamp.
     *
     * @return value of the timestamp that was backed up, if applicable
     */
    public synchronized long backupExistingTimestamp(long defaultValue) {
        return clientPool().runWithRetry(client -> {
            BoundData boundData = getCurrentBoundData(client);
            byte[] currentBound = boundData.bound();
            byte[] currentBackupBound = boundData.backupBound();

            if (CassandraTimestampUtils.isValidTimestampData(currentBackupBound)) {
                // Backup bound has been updated!
                Preconditions.checkState(!CassandraTimestampUtils.isValidTimestampData(currentBound),
                        "We had both backup and active bounds readable! This is unexpected; please contact support.");
                log.info("[BACKUP] Didn't backup, because there is already a backup bound.");
                return PtBytes.toLong(currentBackupBound);
            }

            byte[] backupValue = MoreObjects.firstNonNull(currentBound, PtBytes.toBytes(defaultValue));
            ByteBuffer casQueryBuffer = CassandraTimestampUtils.constructCheckAndSetMultipleQuery(
                    ImmutableMap.of(
                            CassandraTimestampUtils.ROW_AND_COLUMN_NAME,
                            Pair.create(currentBound, CassandraTimestampUtils.INVALIDATED_VALUE.toByteArray()),
                            CassandraTimestampUtils.BACKUP_COLUMN_NAME,
                            Pair.create(currentBackupBound, backupValue)));
            executeQueryUnchecked(client, casQueryBuffer);
            return PtBytes.toLong(backupValue);
        });
    }

    /**
     * Restores a backup of an existing timestamp.
     *
     * This may be implemented following the inverse of the backup process:
     *  - Read the value of the timestamp. If this is readable, skip.
     *  - Read the value of the backup (BT). If this is unreadable, fail.
     *  - Do a conditional unlogged batch (safe since the row key is the same)
     *    - CAS the backup timestamp, from BT, to the empty byte array (deletion marker).
     *    - CAS the main timestamp, expecting INVALIDATED_VALUE, to BT.
     */
    public synchronized void restoreFromBackup() {
        clientPool().runWithRetry(client -> {
            BoundData boundData = getCurrentBoundData(client);
            byte[] currentBound = boundData.bound();
            byte[] currentBackupBound = boundData.backupBound();

            if (CassandraTimestampUtils.isValidTimestampData(currentBound)) {
                Preconditions.checkState(currentBackupBound == null
                                || !CassandraTimestampUtils.isValidTimestampData(currentBackupBound),
                        "We had both backup and active bounds readable! This is unexpected; please contact support.");
                log.info("[RESTORE] Didn't restore from backup, because the current timestamp is already readable.");
                return null;
            }

            ByteBuffer casQueryBuffer = CassandraTimestampUtils.constructCheckAndSetMultipleQuery(
                    ImmutableMap.of(
                            CassandraTimestampUtils.ROW_AND_COLUMN_NAME,
                            Pair.create(CassandraTimestampUtils.INVALIDATED_VALUE.toByteArray(), currentBackupBound),
                            CassandraTimestampUtils.BACKUP_COLUMN_NAME,
                            Pair.create(currentBackupBound, PtBytes.EMPTY_BYTE_ARRAY)));
            executeQueryUnchecked(client, casQueryBuffer);
            return null;
        });
    }

    private void checkOperationWasApplied(Optional<Long> expected, long target, CqlResult result) {
        if (!CassandraTimestampUtils.wasOperationApplied(result)) {
            throw constructConcurrentTimestampStoreException(expected, target, result);
        }
    }

    private CassandraClientPool clientPool() {
        return cassandraKeyValueService.clientPool;
    }

    private TracingQueryRunner queryRunner() {
        return cassandraKeyValueService.queryRunner;
    }

    private CqlResult executeQueryUnchecked(Cassandra.Client client, ByteBuffer queryBuffer) {
        try {
            return queryRunner().run(client,
                    AtlasDbConstants.TIMESTAMP_TABLE,
                    () -> client.execute_cql3_query(queryBuffer, Compression.NONE, ConsistencyLevel.QUORUM));
        } catch (TException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    private BoundData getCurrentBoundData(Cassandra.Client client) {
        ByteBuffer selectQuery = CassandraTimestampUtils.constructSelectFromTimestampTableQuery();
        CqlResult existingData = executeQueryUnchecked(client, selectQuery);
        Map<String, byte[]> columnarResults = CassandraTimestampUtils.getValuesFromSelectionResult(existingData);

        return ImmutableBoundData.builder()
                .bound(columnarResults.get(CassandraTimestampUtils.ROW_AND_COLUMN_NAME))
                .backupBound(columnarResults.get(CassandraTimestampUtils.BACKUP_COLUMN_NAME))
                .build();
    }

    private static ConcurrentModificationException constructConcurrentTimestampStoreException(
            Optional<Long> expected,
            long target,
            CqlResult result) {
        Optional<Long> actual = CassandraTimestampUtils.getLongValueFromApplicationResult(result);

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

    @Value.Immutable
    interface BoundData {
        @Nullable
        byte[] bound();

        @Nullable
        byte[] backupBound();
    }
}

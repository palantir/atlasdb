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
import com.palantir.util.Pair;

public class CassandraTimestampBackupRunner {
    private static final Logger log = LoggerFactory.getLogger(CassandraTimestampBackupRunner.class);

    private final CassandraKeyValueService cassandraKeyValueService;

    public CassandraTimestampBackupRunner(CassandraKeyValueService cassandraKeyValueService) {
        this.cassandraKeyValueService = cassandraKeyValueService;
    }

    /**
     * Creates the timestamp table, if it doesn't already exist.
     */
    @Idempotent
    public void createTimestampTable() {
        cassandraKeyValueService.createTable(
                AtlasDbConstants.TIMESTAMP_TABLE,
                CassandraTimestampUtils.TIMESTAMP_TABLE_METADATA.persistToBytes());
    }

    /**
     * Writes a backup of the existing timestamp to the database, if none exists. After this backup, this timestamp
     * service can no longer be used until a restore. Note that the value returned is the value that was backed up;
     * multiple calls to this method are safe, and will return the backed up value.
     *
     * This method assumes that the timestamp table exists.
     *
     * @param defaultValue value to backup if the timestamp table is empty
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

            Preconditions.checkState(currentBound == null || CassandraTimestampUtils.isValidTimestampData(currentBound),
                    "The timestamp is unreadable, though the backup is also unreadable! Please contact support.");
            byte[] backupValue = MoreObjects.firstNonNull(currentBound, PtBytes.toBytes(defaultValue));
            Map<String, Pair<byte[], byte[]>> casMap =
                    ImmutableMap.of(
                            CassandraTimestampUtils.ROW_AND_COLUMN_NAME,
                            Pair.create(currentBound, CassandraTimestampUtils.INVALIDATED_VALUE.toByteArray()),
                            CassandraTimestampUtils.BACKUP_COLUMN_NAME,
                            Pair.create(currentBackupBound, backupValue));

            ByteBuffer casQueryBuffer = CassandraTimestampUtils.constructCheckAndSetMultipleQuery(casMap);

            CqlResult casResult = executeQueryUnchecked(client, casQueryBuffer);
            CassandraTimestampUtils.verifyCompatible(casResult, casMap);
            log.info("[BACKUP] Backed up the value {}", currentBackupBound);
            return PtBytes.toLong(backupValue);
        });
    }

    /**
     * Restores a backup of an existing timestamp, if possible. Note that this method will throw if the timestamp
     * backup is unreadable, *and* the current bound is also unreadable.
     *
     * This method assumes that the timestamp table exists.
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

            Preconditions.checkState(CassandraTimestampUtils.isValidTimestampData(currentBackupBound),
                    "The timestamp backup is unreadable, so we cannot restore our backup. Please contact support.");
            Map<String, Pair<byte[], byte[]>> casMap = ImmutableMap.of(
                    CassandraTimestampUtils.ROW_AND_COLUMN_NAME,
                    Pair.create(CassandraTimestampUtils.INVALIDATED_VALUE.toByteArray(), currentBackupBound),
                    CassandraTimestampUtils.BACKUP_COLUMN_NAME,
                    Pair.create(currentBackupBound, PtBytes.EMPTY_BYTE_ARRAY));
            ByteBuffer casQueryBuffer = CassandraTimestampUtils.constructCheckAndSetMultipleQuery(casMap);

            CqlResult casResult = executeQueryUnchecked(client, casQueryBuffer);
            CassandraTimestampUtils.verifyCompatible(casResult, casMap);
            log.info("[RESTORE] Restored the value {}", currentBackupBound);
            return null;
        });
    }

    private CqlResult executeQueryUnchecked(Cassandra.Client client, ByteBuffer query) {
        try {
            return queryRunner().run(client,
                    AtlasDbConstants.TIMESTAMP_TABLE,
                    () -> client.execute_cql3_query(query, Compression.NONE, ConsistencyLevel.QUORUM));
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

    private CassandraClientPool clientPool() {
        return cassandraKeyValueService.clientPool;
    }

    private TracingQueryRunner queryRunner() {
        return cassandraKeyValueService.getTracingQueryRunner();
    }

    @Value.Immutable
    interface BoundData {
        @Nullable
        byte[] bound();

        @Nullable
        byte[] backupBound();
    }
}

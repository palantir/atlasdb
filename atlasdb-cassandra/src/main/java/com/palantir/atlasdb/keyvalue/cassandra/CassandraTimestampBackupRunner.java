/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.cassandra;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.base.Throwables;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.util.Pair;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.thrift.TException;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public void ensureTimestampTableExists() {
        cassandraKeyValueService.createTable(
                AtlasDbConstants.TIMESTAMP_TABLE,
                CassandraTimestampBoundStore.TIMESTAMP_TABLE_METADATA.persistToBytes());
    }

    /**
     * Writes a backup of the existing timestamp to the database, if none exists. After this backup, this timestamp
     * service can no longer be used until a restore. Note that the value returned is the value that was backed up;
     * multiple calls to this method are safe, and will return the backed up value.
     *
     * This method assumes that the timestamp table exists.
     *
     * @return value of the timestamp that was backed up, if applicable
     */
    public synchronized long backupExistingTimestamp() {
        return clientPool().runWithRetry(client -> {
            BoundData boundData = getCurrentBoundData(client);
            byte[] currentBound = boundData.bound();
            byte[] currentBackupBound = boundData.backupBound();

            BoundReadability boundReadability = checkReadability(boundData);
            if (boundReadability == BoundReadability.BACKUP) {
                log.info(
                        "[BACKUP] Didn't backup, because there is already a valid backup of {}.",
                        SafeArg.of("currentBackupBound", PtBytes.toLong(currentBackupBound)));
                return PtBytes.toLong(currentBackupBound);
            }

            byte[] backupValue =
                    MoreObjects.firstNonNull(currentBound, PtBytes.toBytes(CassandraTimestampUtils.INITIAL_VALUE));

            Map<String, Pair<byte[], byte[]>> casMap = ImmutableMap.of(
                    CassandraTimestampUtils.ROW_AND_COLUMN_NAME,
                    Pair.create(currentBound, CassandraTimestampUtils.INVALIDATED_VALUE.toByteArray()),
                    CassandraTimestampUtils.BACKUP_COLUMN_NAME,
                    Pair.create(currentBackupBound, backupValue));
            executeAndVerifyCas(client, casMap);
            log.info("[BACKUP] Backed up the value {}", SafeArg.of("backupValue", PtBytes.toLong(backupValue)));
            return PtBytes.toLong(backupValue);
        });
    }

    /**
     * Restores a backup of an existing timestamp, if possible.
     *
     * This method assumes that the timestamp table exists.
     */
    public synchronized void restoreFromBackup() {
        clientPool().runWithRetry(client -> {
            BoundData boundData = getCurrentBoundData(client);
            byte[] currentBound = boundData.bound();
            byte[] currentBackupBound = boundData.backupBound();

            BoundReadability boundReadability = checkReadability(boundData);
            if (boundReadability == BoundReadability.BOUND) {
                if (currentBound == null) {
                    log.info("[RESTORE] Didn't restore, because the current bound is empty (and thus readable).");
                } else {
                    log.info(
                            "[RESTORE] Didn't restore, because the current bound is readable with the value {}.",
                            SafeArg.of("currentBound", PtBytes.toLong(currentBound)));
                }
                return null;
            }

            Map<String, Pair<byte[], byte[]>> casMap = ImmutableMap.of(
                    CassandraTimestampUtils.ROW_AND_COLUMN_NAME,
                    Pair.create(CassandraTimestampUtils.INVALIDATED_VALUE.toByteArray(), currentBackupBound),
                    CassandraTimestampUtils.BACKUP_COLUMN_NAME,
                    Pair.create(currentBackupBound, PtBytes.EMPTY_BYTE_ARRAY));
            executeAndVerifyCas(client, casMap);
            log.info(
                    "[RESTORE] Restored the value {}",
                    SafeArg.of("currentBackupBound", PtBytes.toLong(currentBackupBound)));
            return null;
        });
    }

    private BoundReadability checkReadability(BoundData boundData) {
        BoundReadability boundReadability = getReadability(boundData);

        Preconditions.checkState(
                boundReadability != BoundReadability.BOTH,
                "We had both backup and active timestamp bounds readable! This is unexpected. Please contact support.");
        Preconditions.checkState(
                boundReadability != BoundReadability.NEITHER,
                "We had an unreadable active timestamp bound with no backup! This is unexpected. Please contact "
                        + "support.");
        return boundReadability;
    }

    private BoundReadability getReadability(BoundData boundData) {
        boolean boundReadable =
                boundData.bound() == null || CassandraTimestampUtils.isValidTimestampData(boundData.bound());
        boolean backupBoundReadable = CassandraTimestampUtils.isValidTimestampData(boundData.backupBound());

        if (boundReadable) {
            return backupBoundReadable ? BoundReadability.BOTH : BoundReadability.BOUND;
        }
        return backupBoundReadable ? BoundReadability.BACKUP : BoundReadability.NEITHER;
    }

    private BoundData getCurrentBoundData(CassandraClient client) {
        checkTimestampTableExists();

        CqlQuery selectQuery = CassandraTimestampUtils.constructSelectFromTimestampTableQuery();
        CqlResult existingData = executeQueryUnchecked(client, selectQuery);
        Map<String, byte[]> columnarResults = CassandraTimestampUtils.getValuesFromSelectionResult(existingData);

        return ImmutableBoundData.builder()
                .bound(columnarResults.get(CassandraTimestampUtils.ROW_AND_COLUMN_NAME))
                .backupBound(columnarResults.get(CassandraTimestampUtils.BACKUP_COLUMN_NAME))
                .build();
    }

    private void checkTimestampTableExists() {
        CassandraTables cassandraTables = cassandraKeyValueService.getCassandraTables();
        Preconditions.checkState(
                cassandraTables.getExisting().contains(AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName()),
                "[BACKUP/RESTORE] Tried to get timestamp bound data when the timestamp table didn't exist!");
    }

    private void executeAndVerifyCas(CassandraClient client, Map<String, Pair<byte[], byte[]>> casMap) {
        CqlQuery casQueryBuffer = CassandraTimestampUtils.constructCheckAndSetMultipleQuery(casMap);

        CqlResult casResult = executeQueryUnchecked(client, casQueryBuffer);
        CassandraTimestampUtils.verifyCompatible(casResult, casMap);
    }

    private CqlResult executeQueryUnchecked(CassandraClient client, CqlQuery query) {
        try {
            return queryRunner()
                    .run(
                            client,
                            AtlasDbConstants.TIMESTAMP_TABLE,
                            () -> client.execute_cql3_query(query, Compression.NONE, ConsistencyLevel.QUORUM));
        } catch (TException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    private CassandraClientPool clientPool() {
        return cassandraKeyValueService.getClientPool();
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

    private enum BoundReadability {
        BOUND,
        BACKUP,
        BOTH,
        NEITHER
    }
}

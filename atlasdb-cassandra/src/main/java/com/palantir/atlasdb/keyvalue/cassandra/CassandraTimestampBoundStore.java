/**
 * Copyright 2015 Palantir Technologies
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
import java.util.UUID;

import javax.annotation.concurrent.GuardedBy;

import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
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
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;
import com.palantir.timestamp.DebugLogger;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.TimestampBoundStore;
import com.palantir.util.debug.ThreadDumps;

public final class CassandraTimestampBoundStore implements TimestampBoundStore {
    private static final Logger log = LoggerFactory.getLogger(CassandraTimestampBoundStore.class);

    private static final long CASSANDRA_TIMESTAMP = 0L;
    private static final String ROW_AND_COLUMN_NAME = "ts";

    public static final TableMetadata TIMESTAMP_TABLE_METADATA = new TableMetadata(
            NameMetadataDescription.create(ImmutableList.of(
                    new NameComponentDescription("timestamp_name", ValueType.STRING))),
            new ColumnMetadataDescription(ImmutableList.of(
                new NamedColumnDescription(
                        ROW_AND_COLUMN_NAME,
                        "current_max_ts",
                        ColumnValueDescription.forType(ValueType.FIXED_LONG)))),
            ConflictHandler.IGNORE_ALL);

    private static final long INITIAL_VALUE = 10000L;

    @GuardedBy("this")
    private long currentLimit = -1;
    private boolean startingUp = true;

    private final UUID id;
    private final CassandraClientPool clientPool;

    public static TimestampBoundStore create(CassandraKeyValueService kvs) {
        kvs.createTable(AtlasDbConstants.TIMESTAMP_TABLE, TIMESTAMP_TABLE_METADATA.persistToBytes());
        return new CassandraTimestampBoundStore(kvs.clientPool);
    }

    private CassandraTimestampBoundStore(CassandraClientPool clientPool) {
        DebugLogger.logger.info(
                "Creating CassandraTimestampBoundStore object on thread {}. This should only happen once.",
                Thread.currentThread().getName());
        this.clientPool = Preconditions.checkNotNull(clientPool, "clientPool cannot be null");
        this.id = UUID.randomUUID();
        DebugLogger.logger.info("The ID of this store is {}.", id);
    }

    @VisibleForTesting
    UUID getId() {
        return id;
    }

    @Override
    public synchronized long getUpperLimit() {
        DebugLogger.logger.debug("[GET] Getting upper limit");
        return clientPool.runWithRetry(client -> {
            ByteBuffer rowName = getRowName();
            ColumnPath columnPath = new ColumnPath(AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName());
            columnPath.setColumn(getColumnName());
            ColumnOrSuperColumn result;
            try {
                result = client.get(rowName, columnPath, ConsistencyLevel.LOCAL_QUORUM);
            } catch (NotFoundException e) {
                result = null;
            } catch (TException e) {
                throw Throwables.throwUncheckedException(e);
            }
            if (result == null) {
                DebugLogger.logger.info("[GET] Null result, setting timestamp limit to {}", INITIAL_VALUE);
                cas(client, makeColumnForIdAndBound(null, null), null, INITIAL_VALUE);
                return INITIAL_VALUE;
            }
            TimestampBoundStoreEntry timestampBoundStoreEntry =
                    TimestampBoundStoreEntry.createFromColumn(result.getColumn());
            if (!startingUp && !id.equals(timestampBoundStoreEntry.getId())) {
                throwGettingMultipleRunningTimestampServiceError(timestampBoundStoreEntry);
            }
            currentLimit = timestampBoundStoreEntry.getTimestamp();
            DebugLogger.logger.info("[GET] Setting cached timestamp limit to {}.", currentLimit);
            return currentLimit;
        });
    }

    @Override
    public synchronized void storeUpperLimit(final long limit) {
        DebugLogger.logger.debug("[PUT] Storing upper limit of {}.", limit);
        clientPool.runWithRetry((FunctionCheckedException<Client, Void, RuntimeException>) client -> {
            if (startingUp && currentLimit == -1) {
                cas(client, makeColumnForIdAndBound(null, null), null, limit);
            } else {
                cas(client, makeColumnForIdAndBound(id, currentLimit), currentLimit, limit);
            }
            return null;
        });
    }

    private void cas(Client client, Column oldColumn, Long oldVal, long newVal) {
        CASResult result = updateTimestampInDb(client, oldColumn, oldVal, newVal);
        if (result.isSuccess()) {
            DebugLogger.logger.info("[CAS] Setting cached limit to {}.", newVal);
            currentLimit = newVal;
            startingUp = false;
        } else {
            TimestampBoundStoreEntry timestampBoundStoreEntry = TimestampBoundStoreEntry.createFromCasResult(result);
            if (result.getCurrent_values().isEmpty()) {
                DebugLogger.logger.info("[CAS] The DB is empty!");
                addProcessInfoAndThrow(timestampBoundStoreEntry, "No limit in DB!");
            }
            /*
             * For the cas to succeed, the existing entry in the DB must be this.id_this.currentLimit. If that is not
             * the case, we still want to succeed if:
             *   1. id in the DB equals this.id -- indicates miscommunication about what was written to the DB,
             *   but is not a case of multiple running timestamps; or
             *   2. limit in the DB equals the expected limit, but there is no id/id does not match and this store has
             *   not stored a bound yet -- this is the case when we startup.
             */
            if (sameIdOrConsistentStartUp(timestampBoundStoreEntry)) {
                Column expectedColumn = makeColumn(timestampBoundStoreEntry.getByteValue());
                cas(client, expectedColumn, timestampBoundStoreEntry.getTimestamp(), newVal);
            } else {
                throwStoringMultipleRunningTimestampServiceError(oldVal, newVal, timestampBoundStoreEntry);
            }
        }
    }

    private CASResult updateTimestampInDb(Client client, Column oldColumn, Long oldVal, long newVal) {
        CASResult result;
        DebugLogger.logger.info("[CAS] Trying to set upper limit from {} to {}.", oldVal, newVal);
        try {
            result = client.cas(
                    getRowName(),
                    AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName(),
                    oldVal == null ? ImmutableList.of() : ImmutableList.of(oldColumn),
                    ImmutableList.of(makeColumnForIdAndBound(id, newVal)),
                    ConsistencyLevel.SERIAL,
                    ConsistencyLevel.EACH_QUORUM);
            return result;
        } catch (Exception e) {
            log.error("[CAS] Error trying to set from {} to {}", oldVal, newVal, e);
            DebugLogger.logger.error("[CAS] Error trying to set from {} to {}", oldVal, newVal, e);
            throw Throwables.throwUncheckedException(e);
        }
    }

    private boolean sameIdOrConsistentStartUp(TimestampBoundStoreEntry timestampBoundStoreEntry) {
        return id.equals(timestampBoundStoreEntry.getId())
                || (startingUp && timestampBoundStoreEntry.getTimestamp() == currentLimit);
    }

    private Column makeColumnForIdAndBound(UUID idToUse, Long ts) {
        return makeColumn(TimestampBoundStoreEntry.getByteValueForIdAndBound(idToUse, ts));
    }

    private Column makeColumn(byte[] values) {
        Column col = new Column();
        col.setName(getColumnName());
        col.setValue(values);
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

    private void throwGettingMultipleRunningTimestampServiceError(TimestampBoundStoreEntry timestampBoundStoreEntry) {
        String msg = "Detected a concurrent running timestamp service running against this cassandra keyspace.";
        addProcessInfoAndThrow(timestampBoundStoreEntry, msg);
    }

    private void throwStoringMultipleRunningTimestampServiceError(Long oldVal, long newVal,
            TimestampBoundStoreEntry timestampBoundStoreEntry) {
        String msg = "Unable to CAS from {} to {}."
                + " Timestamp limit changed underneath us (limit in memory: {}, stored in DB: {}). This may"
                + " indicate that another timestamp service is running against this cassandra keyspace."
                + " This is likely caused by multiple copies of a service running without a configured set of"
                + " leaders or a CLI being run with an embedded timestamp service against an already running"
                + " service.";
        String formattedMsg = String.format(replaceBracesWithStringFormatSpecifier(msg), oldVal, newVal,
                currentLimit, timestampBoundStoreEntry.getTimestampAsString());
        addProcessInfoAndThrow(timestampBoundStoreEntry, formattedMsg);
    }

    private void addProcessInfoAndThrow(TimestampBoundStoreEntry timestampBoundStoreEntry, String msg) {
        String processInfo = " This process's ID: {}, ID in DB: {}.";
        String fullMessage = String.format(replaceBracesWithStringFormatSpecifier(msg + processInfo),
                id, timestampBoundStoreEntry.getIdAsString());

        MultipleRunningTimestampServiceError err = new MultipleRunningTimestampServiceError(fullMessage);
        log.error(fullMessage, err);
        DebugLogger.logger.error(fullMessage, err);
        DebugLogger.logger.error("Thread dump: {}", ThreadDumps.programmaticThreadDump());
        throw err;
    }

    private String replaceBracesWithStringFormatSpecifier(String msg) {
        return msg.replaceAll("\\{\\}", "%s");
    }
}

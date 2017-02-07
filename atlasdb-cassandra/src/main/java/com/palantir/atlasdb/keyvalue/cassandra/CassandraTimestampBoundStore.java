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
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.annotation.concurrent.GuardedBy;

import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

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

    private static final byte[] ROW_TIMESTAMP_ARRAY = CassandraKeyValueServices.makeCompositeBuffer(
            PtBytes.toBytes(ROW_AND_COLUMN_NAME), CASSANDRA_TIMESTAMP).array();
    private static final ByteBuffer BYTE_BUFFER_ROW_NAME = ByteBuffer.wrap(PtBytes.toBytes(ROW_AND_COLUMN_NAME));

    private static final NamedColumnDescription COLUMN_DESCRIPTION = new NamedColumnDescription(
            ROW_AND_COLUMN_NAME, "current_max_ts", ColumnValueDescription.forType(ValueType.FIXED_LONG));
    private static final NameMetadataDescription NAME_METADATA_DESCRIPTION = NameMetadataDescription.create(
            ImmutableList.of(new NameComponentDescription("timestamp_name", ValueType.STRING)));
    private static final ColumnMetadataDescription COLUMN_METADATA_DESCRIPTION = new ColumnMetadataDescription(
            ImmutableList.of(COLUMN_DESCRIPTION));

    public static final TableMetadata TIMESTAMP_TABLE_METADATA = new TableMetadata(
            NAME_METADATA_DESCRIPTION, COLUMN_METADATA_DESCRIPTION, ConflictHandler.IGNORE_ALL);

    private final UUID id;
    private final CassandraClientPool clientPool;

    @GuardedBy("this")
    private long currentLimit = -1;
    private boolean startingUp = true;

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
        DebugLogger.logger.info("The ID of this timestamp service is {}.", id);
    }

    @VisibleForTesting
    UUID getId() {
        return id;
    }

    @Override
    public synchronized long getUpperLimit() {
        DebugLogger.logger.debug("[GET] Getting upper limit");
        return clientPool.runWithRetry(client -> {
                    ColumnPath columnPath = new ColumnPath(AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName());
                    columnPath.setColumn(ROW_TIMESTAMP_ARRAY);

                    Optional<Column> column = getColumnIfExists(client, BYTE_BUFFER_ROW_NAME, columnPath);
                    TimestampBoundStoreEntry entryInDb = TimestampBoundStoreEntry.createFromColumn(column);
                    entryInDb = migrateIfStartingUp(entryInDb);
                    checkValidId(entryInDb);
                    setCurrentLimit("[GET]", entryInDb);
                    return currentLimit;
                }
        );
    }

    @Override
    public synchronized void storeUpperLimit(final long limit) {
        DebugLogger.logger.debug("[PUT] Storing upper limit of {}.", limit);
        casWithRetry(TimestampBoundStoreEntry.create(currentLimit, id), TimestampBoundStoreEntry.create(limit, id));
    }

    private Optional<Column> getColumnIfExists(Client client, ByteBuffer rowName, ColumnPath columnPath) {
        Optional<Column> columnInDb;
        try {
            columnInDb = Optional.of(client.get(rowName, columnPath, ConsistencyLevel.LOCAL_QUORUM).getColumn());
        } catch (NotFoundException e) {
            columnInDb = Optional.empty();
        } catch (TException e) {
            throw Throwables.throwUncheckedException(e);
        }
        return columnInDb;
    }

    private TimestampBoundStoreEntry migrateIfStartingUp(TimestampBoundStoreEntry entryInDb) {
        if (startingUp) {
            DebugLogger.logger.info("[GET] The service is starting up. Attempting to get timestamp bound from the DB "
                    + "and resetting it with this process's ID!");
            TimestampBoundStoreEntry newEntry = TimestampBoundStoreEntry.create(entryInDb.getTimestamp(), id);
            casWithRetry(entryInDb, newEntry);
            startingUp = false;
            return newEntry;
        }
        return entryInDb;
    }

    private void casWithRetry(TimestampBoundStoreEntry entryInDb, TimestampBoundStoreEntry newEntry) {
        clientPool.runWithRetry((FunctionCheckedException<Client, Void, RuntimeException>) client -> {
            cas(client, entryInDb, newEntry);
            return null;
        });
    }

    private void cas(Client client, TimestampBoundStoreEntry entryInDb, TimestampBoundStoreEntry newEntry) {
        checkLimitNotDecreasing(newEntry);
        CASResult result = updateTimestampInDb(client, entryInDb, newEntry);
        if (result.isSuccess()) {
            setCurrentLimit("[CAS]", newEntry);
        } else {
            retryCasIfMatchingId(client, newEntry, result);
        }
    }

    private void checkLimitNotDecreasing(TimestampBoundStoreEntry newEntry) {
        if (currentLimit > newEntry.getTimestamp()) {
            throwNewTimestampTooSmallException(newEntry);
        }
    }

    private CASResult updateTimestampInDb(Client client, TimestampBoundStoreEntry entryInDb,
            TimestampBoundStoreEntry newEntry) {
        CASResult result;
        DebugLogger.logger.info("[CAS] Trying to set upper limit from {} to {}.", entryInDb.getTimestampAsString(),
                newEntry.getTimestampAsString());
        try {
            result = client.cas(
                    BYTE_BUFFER_ROW_NAME,
                    AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName(),
                    makeListOfColumnsFromEntry(entryInDb),
                    makeListOfColumnsFromEntry(newEntry),
                    ConsistencyLevel.SERIAL,
                    ConsistencyLevel.EACH_QUORUM);
            return result;
        } catch (Exception e) {
            throwUpdateUncheckedException(entryInDb, newEntry, e);
            return null;
        }
    }

    private void setCurrentLimit(String type, TimestampBoundStoreEntry newEntry) {
        checkLimitNotDecreasing(newEntry);
        currentLimit = newEntry.getTimestamp();
        DebugLogger.logger.info("{} Setting cached timestamp limit to {}.", type, currentLimit);
    }

    private void retryCasIfMatchingId(Client client, TimestampBoundStoreEntry newEntry, CASResult result) {
        TimestampBoundStoreEntry entryInDb = TimestampBoundStoreEntry.createFromCasResult(result);
        if (entryInDb.idMatches(id)) {
            setCurrentLimit("[CAS]", entryInDb);
            entryInDb = TimestampBoundStoreEntry.create(currentLimit, id);
            cas(client, entryInDb, newEntry);
        } else {
            throwStoringMultipleRunningTimestampServiceError(entryInDb, newEntry);
        }
    }

    private void checkValidId(TimestampBoundStoreEntry entryInDb) {
        if (!entryInDb.idMatches(id)) {
            throwGettingMultipleRunningTimestampServiceError(entryInDb);
        }
    }

    private List<Column> makeListOfColumnsFromEntry(TimestampBoundStoreEntry entry) {
        return makeColumn(entry.getByteValue()).map(ImmutableList::of).orElse(ImmutableList.of()).asList();
    }

    private Optional<Column> makeColumn(byte[] values) {
        if (values == null) {
            return Optional.empty();
        }
        Column col = new Column();
        col.setName(ROW_TIMESTAMP_ARRAY);
        col.setValue(values);
        col.setTimestamp(CASSANDRA_TIMESTAMP);
        return Optional.of(col);
    }

    private void throwNewTimestampTooSmallException(TimestampBoundStoreEntry entryInDb) {
        String message = "Cannot set cached timestamp bound value from {} to {}. The bounds must be increasing!";
        String formattedMessage = MessageFormatter.arrayFormat(message, new String[]{
                Long.toString(currentLimit), entryInDb.getTimestampAsString()}).getMessage();
        logMessage(formattedMessage);
        throw new IllegalArgumentException(formattedMessage);
    }

    private void throwGettingMultipleRunningTimestampServiceError(TimestampBoundStoreEntry entryInDb) {
        String message = "Error getting the timestamp limit from the DB: the timestamp service ID {} in the DB"
                + " does not match this service's ID: {}. This may indicate that another timestamp service is"
                + " running against this cassandra keyspace.";
        String formattedMessage = MessageFormatter.arrayFormat(message, new String[]{
                entryInDb.getIdAsString(), id.toString()}).getMessage();
        logMessage(formattedMessage);
        throw new MultipleRunningTimestampServiceError(formattedMessage);
    }

    private void throwStoringMultipleRunningTimestampServiceError(TimestampBoundStoreEntry entryInDb,
            TimestampBoundStoreEntry desiredNewEntry) {
        String message = "Unable to CAS from {} to {}. Timestamp limit changed underneath us or another timestamp"
                + " service ID detected. Limit in memory: {}, this service's ID: {}. Limit stored in DB: {},"
                + " the ID stored in DB: {}. This may indicate that another timestamp service is running against"
                + " this cassandra keyspace. This is likely caused by multiple copies of a service running without"
                + " a configured set of leaders or a CLI being run with an embedded timestamp service against"
                + " an already running service.";
        String formattedMessage = MessageFormatter.arrayFormat(message, new String[]{
                Long.toString(currentLimit),
                desiredNewEntry.getTimestampAsString(),
                Long.toString(currentLimit),
                id.toString(),
                entryInDb.getTimestampAsString(),
                entryInDb.getIdAsString()})
                .getMessage();
        logMessage(formattedMessage);
        throw new MultipleRunningTimestampServiceError(formattedMessage);
    }

    private void throwUpdateUncheckedException(TimestampBoundStoreEntry entryInDb,
            TimestampBoundStoreEntry desiredNewEntry, Exception exception) {
        String message = "[CAS] Error trying to set from {} to {}";
        String formattedMessage = MessageFormatter.arrayFormat(message, new String[]{
                entryInDb.getTimestampAsString(), desiredNewEntry.getTimestampAsString()}).getMessage();
        logMessage(formattedMessage);
        throw Throwables.throwUncheckedException(exception);
    }

    private void logMessage(String formattedMessage) {
        log.error("Error: {}", formattedMessage);
        DebugLogger.logger.error("Error: {}", formattedMessage);
        DebugLogger.logger.error("Thread dump: {}", ThreadDumps.programmaticThreadDump());
    }
}

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
import java.util.ArrayList;
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.common.base.Throwables;
import com.palantir.timestamp.DebugLogger;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.TimestampBoundStore;

public final class CassandraTimestampBoundStore implements TimestampBoundStore {
    private static final long CASSANDRA_TIMESTAMP = 0L;
    private static final byte[] ROW_TIMESTAMP_ARRAY = CassandraKeyValueServices.makeCompositeBuffer(
            PtBytes.toBytes(CassandraTimestampUtils.ROW_AND_COLUMN_NAME), CASSANDRA_TIMESTAMP).array();
    private static final byte[] ID_ROW_TIMESTAMP_ARRAY = CassandraKeyValueServices.makeCompositeBuffer(
            PtBytes.toBytes(CassandraTimestampUtils.ID_ROW_AND_COLUMN_NAME), CASSANDRA_TIMESTAMP).array();
    private static final ByteBuffer ROW_NAME_BYTE_BUFFER =
            ByteBuffer.wrap(PtBytes.toBytes(CassandraTimestampUtils.ROW_AND_COLUMN_NAME));

    private final UUID id;
    private final CassandraClientPool clientPool;

    @GuardedBy("this")
    private long currentLimit = -1;
    private boolean startingUp = true;

    public static TimestampBoundStore create(CassandraKeyValueService kvs) {
        kvs.createTable(AtlasDbConstants.TIMESTAMP_TABLE,
                CassandraTimestampUtils.TIMESTAMP_TABLE_METADATA.persistToBytes());
        return new CassandraTimestampBoundStore(kvs.getClientPool());
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
                    TimestampBoundStoreEntry entryInDb = getStoredTimestampAndMigrateIfStartingUp(client);
                    checkMatchingId(entryInDb);
                    setCurrentLimit("[GET]", entryInDb);
                    return currentLimit;
                }
        );
    }

    @Override
    public synchronized void storeUpperLimit(final long limit) {
        DebugLogger.logger.debug("[PUT] Storing upper limit of {}.", limit);
        casWithRetry(TimestampBoundStoreEntry.create(currentLimit, null),
                TimestampBoundStoreEntry.create(currentLimit, id),
                TimestampBoundStoreEntry.create(limit, null),
                TimestampBoundStoreEntry.create(limit, id));
    }

    private TimestampBoundStoreEntry getStoredTimestampAndMigrateIfStartingUp(Client client) {
        ColumnPath columnPath = new ColumnPath(AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName());
        columnPath.setColumn(ROW_TIMESTAMP_ARRAY);
        Optional<Column> column = getColumnIfExists(client, ROW_NAME_BYTE_BUFFER, columnPath);
        TimestampBoundStoreEntry oldEntry = TimestampBoundStoreEntry.createFromColumn(column);

        columnPath.setColumn(ID_ROW_TIMESTAMP_ARRAY);
        column = getColumnIfExists(client, ROW_NAME_BYTE_BUFFER, columnPath);
        TimestampBoundStoreEntry newEntry = TimestampBoundStoreEntry.createFromColumn(column);

        newEntry = checkSameTimestampOrMigrate(oldEntry, newEntry);
        return newEntry;
    }

    private TimestampBoundStoreEntry checkSameTimestampOrMigrate(TimestampBoundStoreEntry entryWithoutId,
            TimestampBoundStoreEntry entryWithId) {
        if (!startingUp) {
            if (entryWithoutId.getTimestampOrInitialValue() != entryWithId.getTimestampOrInitialValue()) {
                throw new MultipleRunningTimestampServiceError("NOT MATCHING");
            }
            return entryWithId;
        }

        DebugLogger.logger.info("[GET] The service is starting up. Attempting to get timestamp bound from the DB and"
                + " resetting it with this process's ID.");
        long newTimestamp = Long.max(entryWithoutId.getTimestampOrInitialValue(),
                entryWithId.getTimestampOrInitialValue());
        TimestampBoundStoreEntry newEntryWithoutId = TimestampBoundStoreEntry.create(newTimestamp, null);
        TimestampBoundStoreEntry newEntryWithId = TimestampBoundStoreEntry.create(newTimestamp, id);
        casWithRetry(entryWithoutId, entryWithId, newEntryWithoutId, newEntryWithId);
        startingUp = false;
        return newEntryWithId;
    }

    private Optional<Column> getColumnIfExists(Client client, ByteBuffer rowName, ColumnPath columnPath) {
        try {
            return Optional.of(client.get(rowName, columnPath, ConsistencyLevel.LOCAL_QUORUM).getColumn());
        } catch (NotFoundException e) {
            return Optional.empty();
        } catch (TException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private void casWithRetry(TimestampBoundStoreEntry oldWithoutId,
            TimestampBoundStoreEntry oldWithId,
            TimestampBoundStoreEntry newWithoutId,
            TimestampBoundStoreEntry newWithId) {
        clientPool.runWithRetry(client -> {
            checkLimitNotDecreasing(newWithId);
            CASResult result = updateTimestampInDb(client, oldWithoutId, oldWithId, newWithoutId, newWithId);
            if (result.isSuccess()) {
                setCurrentLimit("[CAS]", newWithId);
            } else {
                retryCasIfMatchingId(newWithoutId, newWithId, result);
            }
            return null;
        });
    }

    private void checkLimitNotDecreasing(TimestampBoundStoreEntry newEntry) {
        if (currentLimit > newEntry.getTimestampOrInitialValue()) {
            CassandraTimestampUtils.throwNewTimestampTooSmallException(currentLimit, newEntry);
        }
    }

    private CASResult updateTimestampInDb(Client client,
            TimestampBoundStoreEntry oldWithoutId,
            TimestampBoundStoreEntry oldWithId,
            TimestampBoundStoreEntry newWithoutId,
            TimestampBoundStoreEntry newWithId) {
        CASResult result;
        DebugLogger.logger.info("[CAS] Trying to set upper limit from {} to {}.", oldWithId.getTimestampAsString(),
                newWithId.getTimestampAsString());
        try {
            result = client.cas(
                    ROW_NAME_BYTE_BUFFER,
                    AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName(),
                    makeListOfColumnsFromEntries(oldWithoutId, oldWithId),
                    makeListOfColumnsFromEntries(newWithoutId, newWithId),
                    ConsistencyLevel.SERIAL,
                    ConsistencyLevel.EACH_QUORUM);
            return result;
        } catch (Exception e) {
            CassandraTimestampUtils.logUpdateUncheckedException(oldWithId, newWithId);
            throw Throwables.throwUncheckedException(e);
        }
    }

    private void setCurrentLimit(String type, TimestampBoundStoreEntry newEntry) {
        checkLimitNotDecreasing(newEntry);
        currentLimit = newEntry.getTimestampOrInitialValue();
        DebugLogger.logger.info("{} Setting cached timestamp limit to {}.", type, currentLimit);
    }

    private void retryCasIfMatchingId(TimestampBoundStoreEntry newEntryWithoutId,
            TimestampBoundStoreEntry newEntryWithId,
            CASResult result) {
        List<TimestampBoundStoreEntry> entries = verifyExactlyTwoColumnsAndReturnEntries(result);
        TimestampBoundStoreEntry entryInDbWithoutId = entries.get(0);
        TimestampBoundStoreEntry entryInDbWithId = entries.get(1);
        if (entryInDbWithoutId.getTimestampOrInitialValue() != entryInDbWithId.getTimestampOrInitialValue()) {
            throw new MultipleRunningTimestampServiceError("NOT MATCHING");
        }
        if (entryInDbWithId.idMatches(id)) {
            setCurrentLimit("[CAS]", entryInDbWithId);
            casWithRetry(entryInDbWithoutId, entryInDbWithId, newEntryWithoutId, newEntryWithId);
        } else {
            CassandraTimestampUtils.throwStoringMultipleRunningTimestampServiceError(currentLimit, id,
                    entryInDbWithId, newEntryWithId);
        }
    }

    private List<TimestampBoundStoreEntry> verifyExactlyTwoColumnsAndReturnEntries(CASResult result) {
        if (result.getCurrent_values() == null || result.getCurrent_values().size() != 2) {
            throw new MultipleRunningTimestampServiceError("tsbstore has been tampered with");
        }
        TimestampBoundStoreEntry firstEntry = TimestampBoundStoreEntry.createFromColumn(
                Optional.of(result.getCurrent_values().get(0)));
        TimestampBoundStoreEntry secondEntry = TimestampBoundStoreEntry.createFromColumn(Optional.of(
                result.getCurrent_values().get(1)));
        if (firstEntry.id().isPresent()) {
            return ImmutableList.of(secondEntry, firstEntry);
        }
        return ImmutableList.of(firstEntry, secondEntry);
    }

    private void checkMatchingId(TimestampBoundStoreEntry entryInDb) {
        if (!entryInDb.idMatches(id)) {
            CassandraTimestampUtils.throwGettingMultipleRunningTimestampServiceError(id, entryInDb);
        }
    }

    private List<Column> makeListOfColumnsFromEntries(TimestampBoundStoreEntry entryWithoutId,
            TimestampBoundStoreEntry entryWithId) {
        List<Column> list = new ArrayList<>();
        Optional<Column> column = makeColumn(true, entryWithoutId.getByteValue());
        column.ifPresent(list::add);
        column = makeColumn(false, entryWithId.getByteValue());
        column.ifPresent(list::add);
        return list;
    }

    private Optional<Column> makeColumn(boolean old, byte[] values) {
        if (values == null) {
            return Optional.empty();
        }
        Column col = new Column();
        if (old) {
            col.setName(ROW_TIMESTAMP_ARRAY);
        } else {
            col.setName(ID_ROW_TIMESTAMP_ARRAY);
        }
        col.setValue(values);
        col.setTimestamp(CASSANDRA_TIMESTAMP);
        return Optional.of(col);
    }


}

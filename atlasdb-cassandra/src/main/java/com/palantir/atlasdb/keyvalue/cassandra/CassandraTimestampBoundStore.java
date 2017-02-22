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
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.thrift.TException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.common.base.Throwables;
import com.palantir.timestamp.DebugLogger;
import com.palantir.timestamp.TimestampBoundStore;

public final class CassandraTimestampBoundStore implements TimestampBoundStore {
    private static final long CASSANDRA_TIMESTAMP = 0L;
    static final byte[] NO_ID_TIMESTAMP_ARRAY = CassandraKeyValueServices.makeCompositeBuffer(
            PtBytes.toBytes(CassandraTimestampUtils.ROW_AND_COLUMN_NAME), CASSANDRA_TIMESTAMP).array();
    static final byte[] WITH_ID_TIMESTAMP_ARRAY = CassandraKeyValueServices.makeCompositeBuffer(
            PtBytes.toBytes(CassandraTimestampUtils.WITH_ID_COLUMN_NAME), CASSANDRA_TIMESTAMP).array();
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
        EntryPair entriesInDb = getStoredEntries();
        entriesInDb = migrateIfStartingUp(entriesInDb);
        checkMatchingId(entriesInDb);
        entriesInDb.checkTimestampsMatch();
        setCurrentLimit("GET", entriesInDb);
        return currentLimit;
    }

    @Override
    public synchronized void storeUpperLimit(final long limit) {
        DebugLogger.logger.debug("[PUT] Storing upper limit of {}.", limit);
        casWithRetry(EntryPair.createForTimestampAndId(currentLimit, id), EntryPair.createForTimestampAndId(limit, id));
    }

    private EntryPair getStoredEntries() {
        TimestampBoundStoreEntry entryNoId = getEntryFromColumnInDb(NO_ID_TIMESTAMP_ARRAY);
        TimestampBoundStoreEntry entryWithId = getEntryFromColumnInDb(WITH_ID_TIMESTAMP_ARRAY);
        return EntryPair.create(entryNoId, entryWithId);
    }

    private TimestampBoundStoreEntry getEntryFromColumnInDb(byte[] columnByteArray) {
        ColumnPath columnPath = new ColumnPath(AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName());
        columnPath.setColumn(columnByteArray);
        Optional<Column> column = getColumnIfExists(ROW_NAME_BYTE_BUFFER, columnPath);
        return TimestampBoundStoreEntry.createFromColumn(column);
    }

    private Optional<Column> getColumnIfExists(ByteBuffer rowName, ColumnPath columnPath) {
        return clientPool.runWithRetry(client -> {
            try {
                return Optional.of(client.get(rowName, columnPath, ConsistencyLevel.LOCAL_QUORUM).getColumn());
            } catch (NotFoundException e) {
                return Optional.empty();
            } catch (TException e) {
                throw Throwables.throwUncheckedException(e);
            }
        });
    }

    private EntryPair migrateIfStartingUp(EntryPair entries) {
        if (!startingUp) {
            return entries;
        }
        DebugLogger.logger.info("[GET] The service is starting up. Attempting to get timestamp bound from the DB and"
                + " resetting it with this process's ID. Old format timestamp bound: {}, new format timestamp bound:"
                + " {}. Setting timestamp bound to the higher of the two values or initial value.",
                entries.noId().getTimestampAsString(), entries.withId().getTimestampAsString());
        EntryPair newEntries = EntryPair.createForTimestampAndId(entries.maxTimestamp(), id);
        casWithRetry(entries, newEntries);
        startingUp = false;
        return newEntries;
    }

    private void casWithRetry(EntryPair oldEntries, EntryPair newEntries) {
        checkLimitNotDecreasing(newEntries);
        CASResult result = updateTimestampInDb(oldEntries, newEntries);
        if (result.isSuccess()) {
            setCurrentLimit("CAS", newEntries);
        } else {
            retryCasIfMatchingId(newEntries, result);
        }
    }

    private void checkLimitNotDecreasing(EntryPair newEntries) {
        if (currentLimit > newEntries.getTimestamp()) {
            CassandraTimestampUtils.throwNewTimestampTooSmallException(currentLimit, newEntries);
        }
    }

    private CASResult updateTimestampInDb(EntryPair oldEntries, EntryPair newEntries) {
        return clientPool.runWithRetry(client -> {
            CASResult result;
            DebugLogger.logger.info("[CAS] Trying to set upper limit from {} to {}.",
                    oldEntries.maxTimestamp(), newEntries.getTimestamp());
            try {
                result = client.cas(
                        ROW_NAME_BYTE_BUFFER,
                        AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName(),
                        makeListOfColumnsFromEntries(oldEntries),
                        makeListOfColumnsFromEntries(newEntries),
                        ConsistencyLevel.SERIAL,
                        ConsistencyLevel.EACH_QUORUM);
                return result;
            } catch (Exception e) {
                CassandraTimestampUtils.logUpdateUncheckedException(oldEntries.withId(), newEntries.withId());
                throw Throwables.throwUncheckedException(e);
            }
        });
    }

    private List<Column> makeListOfColumnsFromEntries(EntryPair entries) {
        List<Column> list = new ArrayList<>();
        makeColumn(NO_ID_TIMESTAMP_ARRAY, entries.noId().getByteValue()).ifPresent(list::add);
        makeColumn(WITH_ID_TIMESTAMP_ARRAY, entries.withId().getByteValue()).ifPresent(list::add);
        return list;
    }

    private Optional<Column> makeColumn(byte[] columnByteArray, byte[] values) {
        if (values == null) {
            return Optional.empty();
        }
        Column col = new Column().setName(columnByteArray).setValue(values).setTimestamp(CASSANDRA_TIMESTAMP);
        return Optional.of(col);
    }

    private void setCurrentLimit(String type, EntryPair newEntries) {
        checkLimitNotDecreasing(newEntries);
        currentLimit = newEntries.getTimestamp();
        DebugLogger.logger.info("[{}] Setting cached timestamp limit to {}.", type, currentLimit);
    }

    private void retryCasIfMatchingId(EntryPair newEntries, CASResult result) {
        EntryPair entriesInDb = EntryPair.createFromCasResult(result);
        if (entriesInDb.withId().idMatches(id) && !entriesInDb.noId().id().isPresent()) {
            setCurrentLimit("CAS", entriesInDb);
            casWithRetry(entriesInDb, newEntries);
        } else {
            CassandraTimestampUtils.throwStoringMultipleRunningTimestampServiceError(currentLimit, id,
                    entriesInDb, newEntries);
        }
    }

    private void checkMatchingId(EntryPair entriesInDb) {
        if (!entriesInDb.withId().idMatches(id)) {
            CassandraTimestampUtils.throwGettingMultipleRunningTimestampServiceError(id, entriesInDb);
        }
    }
}

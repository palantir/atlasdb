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
import java.util.Arrays;
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
import org.immutables.value.Value;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.common.base.Throwables;
import com.palantir.timestamp.DebugLogger;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.TimestampBoundStore;

public final class CassandraTimestampBoundStore implements TimestampBoundStore {
    private static final long CASSANDRA_TIMESTAMP = 0L;
    private static final byte[] NO_ID_TIMESTAMP_ARRAY = CassandraKeyValueServices.makeCompositeBuffer(
            PtBytes.toBytes(CassandraTimestampUtils.ROW_AND_COLUMN_NAME), CASSANDRA_TIMESTAMP).array();
    private static final byte[] WITH_ID_TIMESTAMP_ARRAY = CassandraKeyValueServices.makeCompositeBuffer(
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
        return clientPool.runWithRetry(client -> {
                    EntryPair entriesInDb = getStoredTimestampAndMigrateIfStartingUp(client);
                    checkMatchingId(entriesInDb);
                    entriesInDb.checkTimestampsMatch();
                    setCurrentLimit("[GET]", entriesInDb);
                    return currentLimit;
                }
        );
    }

    @Override
    public synchronized void storeUpperLimit(final long limit) {
        DebugLogger.logger.debug("[PUT] Storing upper limit of {}.", limit);
        casWithRetry(EntryPair.createForTimestampAndId(currentLimit, id), EntryPair.createForTimestampAndId(limit, id));
    }

    private EntryPair getStoredTimestampAndMigrateIfStartingUp(Client client) {
        ColumnPath columnPath = new ColumnPath(AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName());
        columnPath.setColumn(NO_ID_TIMESTAMP_ARRAY);
        Optional<Column> column = getColumnIfExists(client, ROW_NAME_BYTE_BUFFER, columnPath);
        TimestampBoundStoreEntry oldEntry = TimestampBoundStoreEntry.createFromColumn(column);

        columnPath.setColumn(WITH_ID_TIMESTAMP_ARRAY);
        column = getColumnIfExists(client, ROW_NAME_BYTE_BUFFER, columnPath);
        TimestampBoundStoreEntry newEntry = TimestampBoundStoreEntry.createFromColumn(column);

        return checkSameTimestampOrMigrate(EntryPair.create(oldEntry, newEntry));
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

    private EntryPair checkSameTimestampOrMigrate(EntryPair entries) {
        if (!startingUp) {
            if (entries.withoutId().getTimestampOrInitialValue() != entries.withoutId().getTimestampOrInitialValue()) {
                CassandraTimestampUtils.throwGetTimestampMismatchError(entries);
            }
            return entries;
        }

        DebugLogger.logger.info("[GET] The service is starting up. Attempting to get timestamp bound from the DB and"
                + " resetting it with this process's ID.");
        EntryPair newEntries = EntryPair.createForTimestampAndId(entries.maxTimestamp(), id);
        casWithRetry(entries, newEntries);
        startingUp = false;
        return newEntries;
    }

    private void casWithRetry(EntryPair oldEntries, EntryPair newEntries) {
        clientPool.runWithRetry(client -> {
            checkLimitNotDecreasing(newEntries);
            CASResult result = updateTimestampInDb(client, oldEntries, newEntries);
            if (result.isSuccess()) {
                setCurrentLimit("[CAS]", newEntries);
            } else {
                retryCasIfMatchingId(newEntries, result);
            }
            return null;
        });
    }

    private void checkLimitNotDecreasing(EntryPair newEntries) {
        if (currentLimit > newEntries.withId().getTimestampOrInitialValue()) {
            CassandraTimestampUtils.throwNewTimestampTooSmallException(currentLimit, newEntries);
        }
    }

    private CASResult updateTimestampInDb(Client client, EntryPair oldEntries, EntryPair newEntries) {
        CASResult result;
        DebugLogger.logger.info("[CAS] Trying to set upper limit from {} to {}.", oldEntries.withId().getTimestampAsString(),
                newEntries.withId().getTimestampAsString());
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
    }

    private void setCurrentLimit(String type, EntryPair newEntries) {
        checkLimitNotDecreasing(newEntries);
        currentLimit = newEntries.withId().getTimestampOrInitialValue();
        DebugLogger.logger.info("{} Setting cached timestamp limit to {}.", type, currentLimit);
    }

    private void retryCasIfMatchingId(EntryPair newEntries, CASResult result) {
        EntryPair entriesInDb = EntryPair.createFromCasResult(result);
        entriesInDb.checkTimestampsMatch();
        if (entriesInDb.withId().idMatches(id) && !entriesInDb.withoutId().id().isPresent()) {
            setCurrentLimit("[CAS]", entriesInDb);
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

    private List<Column> makeListOfColumnsFromEntries(EntryPair entries) {
        List<Column> list = new ArrayList<>();
        Optional<Column> column = makeColumn(true, entries.withoutId().getByteValue());
        column.ifPresent(list::add);
        column = makeColumn(false, entries.withId().getByteValue());
        column.ifPresent(list::add);
        return list;
    }

    private Optional<Column> makeColumn(boolean old, byte[] values) {
        if (values == null) {
            return Optional.empty();
        }
        Column col = new Column();
        if (old) {
            col.setName(NO_ID_TIMESTAMP_ARRAY);
        } else {
            col.setName(WITH_ID_TIMESTAMP_ARRAY);
        }
        col.setValue(values);
        col.setTimestamp(CASSANDRA_TIMESTAMP);
        return Optional.of(col);
    }

    @Value.Immutable
    public abstract static class EntryPair {
        abstract TimestampBoundStoreEntry withoutId();
        abstract TimestampBoundStoreEntry withId();

        public static EntryPair create(TimestampBoundStoreEntry withoutId, TimestampBoundStoreEntry withId) {
            return ImmutableEntryPair.builder()
                    .withoutId(withoutId)
                    .withId(withId)
                    .build();
        }

        public static EntryPair createForTimestampAndId(long ts, UUID id) {
            return create(TimestampBoundStoreEntry.create(ts, null), TimestampBoundStoreEntry.create(ts, id));
        }

        public long maxTimestamp() {
            return Long.max(withoutId().getTimestampOrInitialValue(), withId().getTimestampOrInitialValue());
        }

        public static EntryPair createFromCasResult(CASResult result) {
            if (result.getCurrent_values() == null || result.getCurrent_values().size() != 2) {
                throw new MultipleRunningTimestampServiceError("tsbstore has been tampered with");
            }
            Column column1 = result.getCurrent_values().get(0);
            Column column2 = result.getCurrent_values().get(1);
            if (Arrays.equals(column1.getName(), NO_ID_TIMESTAMP_ARRAY)
                    && Arrays.equals(column2.getName(), WITH_ID_TIMESTAMP_ARRAY)) {
                return create(TimestampBoundStoreEntry.createFromColumn(Optional.of(column1)),
                        TimestampBoundStoreEntry.createFromColumn(Optional.of(column2)));
            } else if (Arrays.equals(column2.getName(), NO_ID_TIMESTAMP_ARRAY)
                    && Arrays.equals(column1.getName(), WITH_ID_TIMESTAMP_ARRAY)) {
                return create(TimestampBoundStoreEntry.createFromColumn(Optional.of(column2)),
                        TimestampBoundStoreEntry.createFromColumn(Optional.of(column1)));
            }
            throw new MultipleRunningTimestampServiceError("tsbstore has been tampered with");
        }

        public void checkTimestampsMatch() {
            if (withoutId().getTimestampOrInitialValue() != withId().getTimestampOrInitialValue()) {
                throw new MultipleRunningTimestampServiceError("NOT MATCHING");
            }
        }
    }
}

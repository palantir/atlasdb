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

import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.thrift.TException;
import org.slf4j.helpers.MessageFormatter;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;
import com.palantir.timestamp.AbstractTimestampBoundStoreWithId;
import com.palantir.timestamp.DebugLogger;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.TimestampBoundStore;
import com.palantir.timestamp.TimestampBoundStoreEntry;
import com.palantir.util.debug.ThreadDumps;

public final class CassandraTimestampBoundStore extends AbstractTimestampBoundStoreWithId {
    private static final long CASSANDRA_TIMESTAMP = 0L;
    private static final byte[] ROW_TIMESTAMP_ARRAY = CassandraKeyValueServices.makeCompositeBuffer(
            PtBytes.toBytes(CassandraTimestampUtils.ROW_AND_COLUMN_NAME), CASSANDRA_TIMESTAMP).array();
    private static final ByteBuffer ROW_NAME_BYTE_BUFFER =
            ByteBuffer.wrap(PtBytes.toBytes(CassandraTimestampUtils.ROW_AND_COLUMN_NAME));

    private final CassandraClientPool clientPool;

    public static TimestampBoundStore create(CassandraKeyValueService kvs) {
        kvs.createTable(AtlasDbConstants.TIMESTAMP_TABLE,
                CassandraTimestampUtils.TIMESTAMP_TABLE_METADATA.persistToBytes());
        return new CassandraTimestampBoundStore(kvs.getClientPool());
    }

    private CassandraTimestampBoundStore(CassandraClientPool clientPool) {
        log.info("Creating CassandraTimestampBoundStore object on thread {}. This should only happen once.",
                Thread.currentThread().getName());
        this.clientPool = Preconditions.checkNotNull(clientPool, "clientPool cannot be null");
        this.id = UUID.randomUUID();
        log.info("The ID of this timestamp service is {}.", id);
    }

    @Override
    protected TimestampBoundStoreEntry getStoredEntry(Object client) {
        ColumnPath columnPath = new ColumnPath(AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName());
        columnPath.setColumn(ROW_TIMESTAMP_ARRAY);
        Optional<Column> column = getColumnIfExists((Client) client, ROW_NAME_BYTE_BUFFER, columnPath);
        return CassandraTimestampBoundStoreEntry.createFromColumn(column);
    }

    @Override
    protected Result updateEntryInDb(Object client,
            TimestampBoundStoreEntry entryInDb, TimestampBoundStoreEntry newEntry) {
        CASResult casResult;
        try {
            casResult = ((Client) client).cas(
                    ROW_NAME_BYTE_BUFFER,
                    AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName(),
                    makeListOfColumnsFromEntry(CassandraTimestampBoundStoreEntry.createFromSuper(entryInDb)),
                    makeListOfColumnsFromEntry(CassandraTimestampBoundStoreEntry.createFromSuper(newEntry)),
                    ConsistencyLevel.SERIAL,
                    ConsistencyLevel.EACH_QUORUM);
            Result result = new Result(casResult.isSuccess(),
                    casResult.isSuccess() ? null : CassandraTimestampBoundStoreEntry.createFromCasResult(casResult));
            return result;
        } catch (Exception e) {
            logUpdateUncheckedException(entryInDb, newEntry);
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    protected long runReturnLong(FunctionCheckedException<?, Long, RuntimeException> function) {
        return clientPool.runWithRetry((FunctionCheckedException<Cassandra.Client, Long, RuntimeException>) function);
    }

    @Override
    protected void runWithNoReturn(FunctionCheckedException<?, Void, RuntimeException> function) {
        clientPool.runWithRetry((FunctionCheckedException<Cassandra.Client, Void, RuntimeException>) function);
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

    private List<Column> makeListOfColumnsFromEntry(CassandraTimestampBoundStoreEntry entry) {
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

    @Override
    public void throwGettingMultipleRunningTimestampServiceError(UUID id, TimestampBoundStoreEntry entryInDb) {
        String message = "Error getting the timestamp limit from the DB: the timestamp service ID {} in the DB"
                + " does not match this service's ID: {}. This may indicate that another timestamp service is"
                + " running against this cassandra keyspace.";
        String formattedMessage = MessageFormatter.arrayFormat(message, new String[]{
                entryInDb.getIdAsString(), id.toString()}).getMessage();
        logMessage(formattedMessage);
        throw new MultipleRunningTimestampServiceError(formattedMessage);
    }

    @Override
    protected void throwStoringMultipleRunningTimestampServiceError(long currentLimit, UUID id,
            TimestampBoundStoreEntry entryInDb, TimestampBoundStoreEntry desiredNewEntry) {
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

    @Override
    protected void throwNewTimestampTooSmallException(long currentLimit, TimestampBoundStoreEntry entryInDb) {
        String message = "Cannot set cached timestamp bound value from {} to {}. The bounds must be increasing!";
        String formattedMessage = MessageFormatter.arrayFormat(message, new String[]{
                Long.toString(currentLimit), entryInDb.getTimestampAsString()}).getMessage();
        logMessage(formattedMessage);
        throw new IllegalArgumentException(formattedMessage);
    }

    public static void logUpdateUncheckedException(TimestampBoundStoreEntry entryInDb,
            TimestampBoundStoreEntry desiredNewEntry) {
        String message = "[CAS] Error trying to set from {} to {}";
        String formattedMessage = MessageFormatter.arrayFormat(message, new String[]{
                entryInDb.getTimestampAsString(), desiredNewEntry.getTimestampAsString()}).getMessage();
        logMessage(formattedMessage);
    }

    private static void logMessage(String formattedMessage) {
        log.error("Error: {}", formattedMessage);
        DebugLogger.logger.error("Error: {}", formattedMessage);
        DebugLogger.logger.error("Thread dump: {}", ThreadDumps.programmaticThreadDump());
    }
}

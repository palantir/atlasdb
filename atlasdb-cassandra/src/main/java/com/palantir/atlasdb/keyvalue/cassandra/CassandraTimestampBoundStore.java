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
import java.util.function.Function;

import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.common.base.Throwables;
import com.palantir.timestamp.AbstractTimestampBoundStoreWithId;
import com.palantir.timestamp.TimestampBoundStoreEntry;

public final class CassandraTimestampBoundStore extends AbstractTimestampBoundStoreWithId {
    private static final long CASSANDRA_TIMESTAMP = 0L;
    private static final byte[] ROW_TIMESTAMP_ARRAY = CassandraKeyValueServices.makeCompositeBuffer(
            PtBytes.toBytes(CassandraTimestampUtils.ROW_AND_COLUMN_NAME), CASSANDRA_TIMESTAMP).array();
    private static final ByteBuffer ROW_NAME_BYTE_BUFFER =
            ByteBuffer.wrap(PtBytes.toBytes(CassandraTimestampUtils.ROW_AND_COLUMN_NAME));

    private final CassandraClientPool clientPool;

    public static AbstractTimestampBoundStoreWithId create(CassandraKeyValueService kvs) {
        kvs.createTable(AtlasDbConstants.TIMESTAMP_TABLE,
                CassandraTimestampUtils.TIMESTAMP_TABLE_METADATA.persistToBytes());
        return new CassandraTimestampBoundStore(kvs.getClientPool());
    }

    private CassandraTimestampBoundStore(CassandraClientPool clientPool) {
        log.info("Creating CassandraTimestampBoundStore object on thread {}. This should only happen once.",
                Thread.currentThread().getName());
        this.clientPool = Preconditions.checkNotNull(clientPool, "clientPool cannot be null");
        super.id = UUID.randomUUID();
        super.INITIAL_VALUE = CassandraTimestampUtils.INITIAL_VALUE;
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
            TimestampBoundStoreEntry entryInDb,
            TimestampBoundStoreEntry newEntry) {
        try {
            CASResult casResult = ((Client) client).cas(
                    ROW_NAME_BYTE_BUFFER,
                    AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName(),
                    makeListOfColumnsFromEntry(CassandraTimestampBoundStoreEntry.createFromSuper(entryInDb)),
                    makeListOfColumnsFromEntry(CassandraTimestampBoundStoreEntry.createFromSuper(newEntry)),
                    ConsistencyLevel.SERIAL,
                    ConsistencyLevel.EACH_QUORUM);
            Result result = casResult.isSuccess() ? Result.success() : Result.failure(
                    CassandraTimestampBoundStoreEntry.createFromCasResult(casResult));
            return result;
        } catch (Exception e) {
            logUpdateUncheckedException(entryInDb, newEntry);
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    protected long runReturnLong(Function<?, Long> function) {
        return clientPool.runWithRetry(client -> ((Function<Client, Long>) function).apply(client));
    }

    @Override
    protected void runWithNoReturn(Function<?, Void> function) {
        clientPool.runWithRetry(client -> ((Function<Client, Void>) function).apply(client));
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
}

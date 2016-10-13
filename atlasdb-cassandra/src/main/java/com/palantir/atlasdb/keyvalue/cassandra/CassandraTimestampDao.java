/**
 * Copyright 2016 Palantir Technologies
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
import java.util.Optional;

import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.thrift.TException;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.common.base.Throwables;

public class CassandraTimestampDao implements TimestampDao {
    private static final long CASSANDRA_TIMESTAMP = 0L;

    private final CassandraClientPool clientPool;

    public CassandraTimestampDao(CassandraClientPool clientPool) {
        this.clientPool = clientPool;
    }

    @Override
    public Optional<Long> getStoredLimit() {
        ByteBuffer rowName = getRowName();
        ColumnPath columnPath = new ColumnPath(AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName());
        columnPath.setColumn(getColumnName());
        ColumnOrSuperColumn cosc = clientPool.runWithRetry(client -> {
            try {
                return client.get(rowName, columnPath, ConsistencyLevel.LOCAL_QUORUM);
            } catch (NotFoundException e) {
                return null;
            } catch (Exception e) {
                throw Throwables.throwUncheckedException(e);
            }
        });
        if (cosc == null) {
            return null;
        }

        Column column = cosc.getColumn();
        return Optional.of(PtBytes.toLong(column.getValue()));
    }

    @Override
    public boolean checkAndSet(Long oldVal, long newVal) {
        CASResult casResult = clientPool.runWithRetry(client -> {
            try {
                return client.cas(
                        getRowName(),
                        AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName(),
                        oldVal == null ? ImmutableList.of()
                                : ImmutableList.of(makeColumn(oldVal)),
                        ImmutableList.of(makeColumn(newVal)),
                        ConsistencyLevel.SERIAL,
                        ConsistencyLevel.EACH_QUORUM);
            } catch (TException e) {
                throw Throwables.throwUncheckedException(e);
            }
        });
        return casResult.isSuccess();
    }

    private Column makeColumn(long ts) {
        Column col = new Column();
        col.setName(getColumnName());
        col.setValue(PtBytes.toBytes(ts));
        col.setTimestamp(CASSANDRA_TIMESTAMP);
        return col;
    }

    private static byte[] getColumnName() {
        return CassandraKeyValueServices
                .makeCompositeBuffer(PtBytes.toBytes(CassandraTimestampBoundStore.ROW_AND_COLUMN_NAME), CASSANDRA_TIMESTAMP)
                .array();
    }

    private static ByteBuffer getRowName() {
        return ByteBuffer.wrap(PtBytes.toBytes(CassandraTimestampBoundStore.ROW_AND_COLUMN_NAME));
    }
}

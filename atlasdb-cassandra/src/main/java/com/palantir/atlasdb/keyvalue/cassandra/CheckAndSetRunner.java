/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class CheckAndSetRunner {
    private final TracingQueryRunner queryRunner;
    private final ConsistencyLevel writeConsistency = ConsistencyLevel.EACH_QUORUM;

    public CheckAndSetRunner(TracingQueryRunner queryRunner) {
        this.queryRunner = queryRunner;
    }

    public CASResult executeCheckAndSet(CassandraClient client, CheckAndSetRequest request)
            throws TException {
        try {
            TableReference table = request.table();
            Cell cell = request.cell();
            long timestamp = AtlasDbConstants.TRANSACTION_TS;

            ByteBuffer rowName = ByteBuffer.wrap(cell.getRowName());
            byte[] colName = CassandraKeyValueServices
                    .makeCompositeBuffer(cell.getColumnName(), timestamp)
                    .array();

            List<Column> oldColumns;
            java.util.Optional<byte[]> oldValue = request.oldValue();
            if (oldValue.isPresent()) {
                oldColumns = ImmutableList.of(makeColumn(colName, oldValue.get(), timestamp));
            } else {
                oldColumns = ImmutableList.of();
            }

            Column newColumn = makeColumn(colName, request.newValue(), timestamp);
            return queryRunner.run(client, table, () -> client.cas(
                    table,
                    rowName,
                    oldColumns,
                    ImmutableList.of(newColumn),
                    ConsistencyLevel.SERIAL,
                    writeConsistency));
        } catch (UnavailableException e) {
            throw new InsufficientConsistencyException(
                    "Check-and-set requires " + writeConsistency + " Cassandra nodes to be up and available.", e);
        }
    }

    private Column makeColumn(byte[] colName, byte[] contents, long timestamp) {
        Column newColumn = new Column();
        newColumn.setName(colName);
        newColumn.setValue(contents);
        newColumn.setTimestamp(timestamp);
        return newColumn;
    }
}

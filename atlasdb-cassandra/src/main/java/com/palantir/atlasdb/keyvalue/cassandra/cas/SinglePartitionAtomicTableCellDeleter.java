/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.cas;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClient;
import com.palantir.atlasdb.keyvalue.cassandra.CqlQuery;
import com.palantir.atlasdb.keyvalue.cassandra.CqlUtilities;
import com.palantir.atlasdb.keyvalue.cassandra.TracingQueryRunner;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.thrift.TException;

public final class SinglePartitionAtomicTableCellDeleter {
    private final TracingQueryRunner runner;
    private final ConsistencyLevel deleteConsistency;

    public SinglePartitionAtomicTableCellDeleter(TracingQueryRunner runner, ConsistencyLevel deleteConsistency) {
        this.runner = runner;
        this.deleteConsistency = deleteConsistency;
    }

    public void deleteFromAtomicTable(CassandraClient client, TableReference table, Cell cell) throws TException {
        runner.run(
                client,
                table,
                () -> client.execute_cql3_query(
                        CqlQuery.builder()
                                .safeQueryFormat(
                                        "DELETE FROM \"%s\" WHERE key=%s AND column1=%s AND column2=%s IF EXISTS;")
                                .addArgs(
                                        LoggingArgs.internalTableName(table),
                                        UnsafeArg.of("row", CqlUtilities.encodeCassandraHexBytes(cell.getRowName())),
                                        UnsafeArg.of(
                                                "column", CqlUtilities.encodeCassandraHexBytes(cell.getColumnName())),
                                        SafeArg.of("cassandraTimestamp", CqlUtilities.CASSANDRA_TIMESTAMP))
                                .build(),
                        Compression.NONE,
                        deleteConsistency));
    }
}

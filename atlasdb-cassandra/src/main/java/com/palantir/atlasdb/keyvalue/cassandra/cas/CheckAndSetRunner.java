/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.MultiCellCheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.MultiCellCheckAndSetRequest.ProposedUpdate;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClient;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraConstants;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServices;
import com.palantir.atlasdb.keyvalue.cassandra.MultiCellCheckAndSetResult;
import com.palantir.atlasdb.keyvalue.cassandra.TracingQueryRunner;
import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;
import com.palantir.common.streams.KeyedStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import okio.ByteString;
import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

public class CheckAndSetRunner {
    private final TracingQueryRunner queryRunner;
    private final ConsistencyLevel writeConsistency = ConsistencyLevel.EACH_QUORUM;

    public CheckAndSetRunner(TracingQueryRunner queryRunner) {
        this.queryRunner = queryRunner;
    }

    public CheckAndSetResult<ByteString> executeCheckAndSet(CassandraClient client, CheckAndSetRequest request)
            throws TException {
        try {
            TableReference table = request.table();
            CqlResult result = queryRunner.run(
                    client,
                    table,
                    () -> client.execute_cql3_query(
                            CheckAndSetQueries.getQueryForRequest(request), Compression.NONE, writeConsistency));
            return CheckAndSetResponseDecoder.decodeCqlResult(result);
        } catch (UnavailableException e) {
            throw new InsufficientConsistencyException(
                    "Check-and-set requires " + writeConsistency + " Cassandra nodes to be up and available.", e);
        }
    }

    public MultiCellCheckAndSetResult<ByteString> executeCheckAndSet(CassandraClient client, MultiCellCheckAndSetRequest request)
            throws TException {
        try {
            // This method differs from the above owing to performance considerations, but different semantics for
            // put-unless-exists style workflows.
            TableReference table = request.tableReference();

            Map<byte[], List<ProposedUpdate>> updatesByRow =
                    request.proposedUpdates().stream().collect(Collectors.groupingBy(update -> update.cell().getRowName()));
            for (Map.Entry<byte[], List<ProposedUpdate>> singleRowUpdates : updatesByRow.entrySet()) {
                CASResult result = queryRunner.run(client, table, () -> client.cas(
                        table,
                        ByteBuffer.wrap(singleRowUpdates.getKey()),
                        singleRowUpdates.getValue().stream().map(t -> prepareCassandraColumn(t.cell(),
                                t.oldValue())).collect(Collectors.toList()),
                        singleRowUpdates.getValue().stream().map(t -> prepareCassandraColumn(t.cell(),
                                t.newValue())).collect(Collectors.toList()),
                        ConsistencyLevel.SERIAL,
                        ConsistencyLevel.QUORUM));
                if (!result.isSuccess()) {
                    return MultiCellCheckAndSetResult.failure(
                            KeyedStream.of(IntStream.range(0, singleRowUpdates.getValue().size())
                                    .boxed())
                                    .mapKeys(index -> singleRowUpdates.getValue().get(index).cell())
                                    .map(index -> result.current_values.get(index).getValue())
                                    .map(ByteString::new)
                                    .collectToMap());
                }
            }
            // all requests successful
            return MultiCellCheckAndSetResult.success(request.relevantCells());
        } catch (UnavailableException e) {
            throw new InsufficientConsistencyException(
                    "Check-and-set requires " + writeConsistency + " Cassandra nodes to be up and available.", e);
        }
    }

    private static Column prepareCassandraColumn(Cell cell, byte[] proposedValue) {
        return new Column(CassandraKeyValueServices.makeCompositeBuffer(
                cell.getColumnName(),
                // Atlas timestamp
                CassandraConstants.CAS_TABLE_TIMESTAMP))
                // Cassandra timestamp
                .setTimestamp(CassandraConstants.CAS_TABLE_TIMESTAMP)
                .setValue(proposedValue);
    }
}

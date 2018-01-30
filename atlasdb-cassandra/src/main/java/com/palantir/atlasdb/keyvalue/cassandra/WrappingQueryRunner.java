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
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.MutationMap;

/**
 * Executes Thrift queries using the supplied {@link TracingQueryRunner}, wrapping {@link UnavailableException} with
 * {@link InsufficientConsistencyException}.
 */
public class WrappingQueryRunner {
    private final TracingQueryRunner queryRunner;

    public WrappingQueryRunner(TracingQueryRunner queryRunner) {
        this.queryRunner = queryRunner;
    }

    Void batchMutate(String kvsMethodName,
            CassandraClient client,
            Set<TableReference> tableRefs,
            MutationMap map,
            ConsistencyLevel consistency) throws TException {
        try {
            return queryRunner.run(client, tableRefs, () -> {
                client.batch_mutate(kvsMethodName, map.toMap(), consistency);
                return null;
            });
        } catch (UnavailableException e) {
            throw new InsufficientConsistencyException(
                    "This batch mutate operation requires " + consistency + " Cassandra nodes to be up and available.",
                    e);
        }
    }

    Map<ByteBuffer, List<ColumnOrSuperColumn>> multiget(
            String kvsMethodName,
            CassandraClient client,
            TableReference tableRef,
            List<ByteBuffer> rowNames,
            SlicePredicate pred,
            ConsistencyLevel consistency) throws TException {
        try {
            return queryRunner.run(client, tableRef,
                    () -> client.multiget_slice(kvsMethodName, tableRef, rowNames, pred, consistency));
        } catch (UnavailableException e) {
            throw new InsufficientConsistencyException(
                    "This get operation requires " + consistency + " Cassandra nodes to be up and available.", e);
        }
    }
}
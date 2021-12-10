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

import com.google.protobuf.ByteString;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClient;
import com.palantir.atlasdb.keyvalue.cassandra.TracingQueryRunner;
import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;
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
}

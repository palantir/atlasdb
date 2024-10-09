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

package com.palantir.atlasdb.keyvalue.cassandra;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.limiter.AtlasClientLimiter;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyPredicate;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

public class RateLimitingCassandraClient implements AutoDelegate_CassandraClient {
    private final CassandraClient delegate;
    private final AtlasClientLimiter clientLimiter;

    public RateLimitingCassandraClient(CassandraClient delegate, AtlasClientLimiter clientLimiter) {
        this.delegate = delegate;
        this.clientLimiter = clientLimiter;
    }

    @Override
    public CassandraClient delegate() {
        return delegate;
    }

    @Override
    public Map<ByteBuffer, List<ColumnOrSuperColumn>> multiget_slice(
            String kvsMethodName,
            TableReference tableRef,
            List<ByteBuffer> keys,
            SlicePredicate predicate,
            ConsistencyLevel consistencyLevel)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        clientLimiter.limitRowsRead(tableRef, keys.size());
        return AutoDelegate_CassandraClient.super.multiget_slice(
                kvsMethodName, tableRef, keys, predicate, consistencyLevel);
    }

    @Override
    public Map<ByteBuffer, List<List<ColumnOrSuperColumn>>> multiget_multislice(
            String kvsMethodName,
            TableReference tableRef,
            List<KeyPredicate> keyPredicates,
            ConsistencyLevel consistencyLevel)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        clientLimiter.limitRowsRead(tableRef, keyPredicates.size());
        return AutoDelegate_CassandraClient.super.multiget_multislice(
                kvsMethodName, tableRef, keyPredicates, consistencyLevel);
    }
}

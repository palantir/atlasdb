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

package com.palantir.atlasdb;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.AutoDelegate_CassandraClient;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClient;
import com.palantir.atlasdb.keyvalue.cassandra.limiter.ClientLimiter;
import java.util.List;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.thrift.TException;

public class RateLimitingCassandraClient implements AutoDelegate_CassandraClient {

    private final CassandraClient delegate;
    private final ClientLimiter clientLimiter;

    public RateLimitingCassandraClient(CassandraClient delegate, ClientLimiter clientLimiter) {
        this.delegate = delegate;
        this.clientLimiter = clientLimiter;
    }

    @Override
    public List<KeySlice> get_range_slices(
            String kvsMethodName,
            TableReference tableRef,
            SlicePredicate predicate,
            KeyRange range,
            ConsistencyLevel consistencyLevel)
            throws TException {
        return clientLimiter.limitGetRangeSlices(
                () -> delegate.get_range_slices(kvsMethodName, tableRef, predicate, range, consistencyLevel));
    }

    @Override
    public CassandraClient delegate() {
        return delegate;
    }
}

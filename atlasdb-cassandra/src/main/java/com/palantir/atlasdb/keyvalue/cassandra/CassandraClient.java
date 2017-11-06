/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
import java.util.concurrent.Callable;

import org.apache.cassandra.thrift.AutoDelegate_Client;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

import com.palantir.atlasdb.qos.AtlasDbQosClient;
import com.palantir.common.base.Throwables;
import com.palantir.processors.AutoDelegate;

/**
 * Wrapper for Cassandra.Client.
 */
@AutoDelegate(typeToExtend = Cassandra.Client.class)
@SuppressWarnings({"checkstyle:all", "DuplicateThrows"}) // :'(
public class CassandraClient extends AutoDelegate_Client {
    private final Cassandra.Client delegate;
    private final AtlasDbQosClient qosClient;

    public CassandraClient(Cassandra.Client delegate, AtlasDbQosClient qosClient) {
        super(delegate.getInputProtocol());
        this.delegate = delegate;
        this.qosClient = qosClient;
    }

    @Override
    public Cassandra.Client delegate() {
        return delegate;
    }

    @Override
    public Map<ByteBuffer, List<ColumnOrSuperColumn>> multiget_slice(List<ByteBuffer> keys, ColumnParent column_parent,
            SlicePredicate predicate, ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        return checkLimitAndCall(() -> super.multiget_slice(keys, column_parent, predicate, consistency_level));
    }

    @Override
    public void batch_mutate(Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        checkLimitAndCall(() -> {
            super.batch_mutate(mutation_map, consistency_level);
            return null;
        });
    }

    @Override
    public CqlResult execute_cql3_query(ByteBuffer query, Compression compression, ConsistencyLevel consistency)
            throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException,
            TException {
        return checkLimitAndCall(() -> super.execute_cql3_query(query, compression, consistency));
    }

    @Override
    public List<KeySlice> get_range_slices(ColumnParent column_parent, SlicePredicate predicate, KeyRange range,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        return checkLimitAndCall(() -> super.get_range_slices(column_parent, predicate, range, consistency_level));
    }

    private <T> T checkLimitAndCall(Callable<T> callable) {
        try {
            qosClient.checkLimit();
            return callable.call();
        } catch (Exception ex) {
            throw Throwables.throwUncheckedException(ex);
        }
    }
}

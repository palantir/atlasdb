/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.async;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyPredicate;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;

/**
 * Callbacks from AsyncClient arrive on the selector thread, which handles network. We choose to shove the actual
 * handling onto an executor.
 * <p>
 * This might not be the right layering.
 */
public final class AsyncCassandraClientImpl implements AsyncCassandraClient {

    private final Cassandra.AsyncClient client;
    private final Executor callbackThreadpool;

    public AsyncCassandraClientImpl(Cassandra.AsyncClient client, Executor callbackThreadpool) {
        this.client = client;
        this.callbackThreadpool = callbackThreadpool;
    }

    @Override
    public ListenableFuture<Map<ByteBuffer, List<List<ColumnOrSuperColumn>>>> multiget_multislice(String kvsMethodName,
            TableReference tableRef, List<KeyPredicate> keyPredicates, ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        try {
            ColumnParent colFam = getColumnParent(tableRef);

            SettableFuture<Map<ByteBuffer, List<List<ColumnOrSuperColumn>>>> result = SettableFuture.create();
            client.multiget_multislice(keyPredicates, colFam, consistency_level,
                    new AsyncMethodCallback<Map<ByteBuffer, List<List<ColumnOrSuperColumn>>>>() {
                        @Override
                        public void onComplete(Map<ByteBuffer, List<List<ColumnOrSuperColumn>>> response) {
                            callbackThreadpool.execute(() -> result.set(response));
                        }

                        @Override
                        public void onError(Exception exception) {
                            callbackThreadpool.execute(() -> result.setException(exception));
                        }
                    });
            return result;
        } catch (Throwable e) {
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public void close() throws IOException {
        // No idea
    }

    private ColumnParent getColumnParent(TableReference tableRef) {
        return new ColumnParent(AbstractKeyValueService.internalTableName(tableRef));
    }
}

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
package com.palantir.atlasdb.keyvalue.cassandra;

import com.palantir.common.base.FunctionCheckedException;
import com.palantir.processors.AutoDelegate;
import java.net.InetSocketAddress;
import java.util.Map;

@AutoDelegate
public interface CassandraClientPool {
    FunctionCheckedException<CassandraClient, Void, Exception> getValidatePartitioner();
    <V, K extends Exception> V runOnHost(InetSocketAddress specifiedHost,
            FunctionCheckedException<CassandraClient, V, K> fn) throws K;
    <V, K extends Exception> V run(FunctionCheckedException<CassandraClient, V, K> fn) throws K;
    <V, K extends Exception> V runWithRetryOnHost(
            InetSocketAddress specifiedHost,
            FunctionCheckedException<CassandraClient, V, K> fn) throws K;
    <V, K extends Exception> V runWithRetry(FunctionCheckedException<CassandraClient, V, K> fn) throws K;
    InetSocketAddress getRandomHostForKey(byte[] key);
    Map<InetSocketAddress, CassandraClientPoolingContainer> getCurrentPools();
    void shutdown();
}

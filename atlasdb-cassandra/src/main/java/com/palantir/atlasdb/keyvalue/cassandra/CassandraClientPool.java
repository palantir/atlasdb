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

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra;

import com.palantir.common.base.FunctionCheckedException;

public interface CassandraClientPool {
    FunctionCheckedException<Cassandra.Client, Void, Exception> getValidatePartitioner();
    <V, K extends Exception> V runOnHost(InetSocketAddress specifiedHost,
            FunctionCheckedException<Cassandra.Client, V, K> fn) throws K;
    <V, K extends Exception> V run(FunctionCheckedException<Cassandra.Client, V, K> fn) throws K;
    <V, K extends Exception> V runWithRetryOnHost(
            InetSocketAddress specifiedHost,
            FunctionCheckedException<Cassandra.Client, V, K> fn) throws K;
    <V, K extends Exception> V runWithRetry(FunctionCheckedException<Cassandra.Client, V, K> fn) throws K;
    InetSocketAddress getAddressForHost(String host) throws UnknownHostException;
    InetSocketAddress getRandomHostForKey(byte[] key);
    Map<InetSocketAddress, CassandraClientPoolingContainer> getCurrentPools();
    void shutdown();
}

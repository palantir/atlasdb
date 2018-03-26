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

import java.util.List;

import org.apache.cassandra.thrift.TokenRange;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.common.base.FunctionCheckedException;

public final class CassandraUtils {
    private CassandraUtils() {}

    public static FunctionCheckedException<CassandraClient, Void, Exception> getValidatePartitioner(
            CassandraKeyValueServiceConfig config) {
        return client -> {
            CassandraVerifier.validatePartitioner(client.describe_partitioner(), config);
            return null;
        };
    }

    public static FunctionCheckedException<CassandraClient, List<TokenRange>, Exception> getDescribeRing(
            CassandraKeyValueServiceConfig config) {
        return client -> client.describe_ring(config.getKeyspaceOrThrow());
    }
}

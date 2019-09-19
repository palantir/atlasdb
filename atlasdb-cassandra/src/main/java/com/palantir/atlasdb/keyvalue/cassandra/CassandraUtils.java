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

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.RetryLimitReachedException;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.exception.AtlasDbDependencyException;
import java.util.List;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.thrift.UnavailableException;

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

    public static AtlasDbDependencyException wrapInIceForDeleteOrRethrow(RetryLimitReachedException ex) {
        if (ex.suppressed(UnavailableException.class) || ex.suppressed(InsufficientConsistencyException.class)) {
            throw new InsufficientConsistencyException("Deleting requires all Cassandra nodes to be available.", ex);
        }
        throw ex;
    }
}

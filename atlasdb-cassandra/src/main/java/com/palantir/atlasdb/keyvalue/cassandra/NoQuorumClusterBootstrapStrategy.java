/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.atlasdb.keyvalue.cassandra.CassandraTopologyValidator.ClusterTopologyResult;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import java.util.Map;

/**
 * Handles topology validation in situations where a quorum of Cassandra servers are not reachable.
 * The intention of this class is only to be used when there is no historical record of the cluster topology as well.
 */
public interface NoQuorumClusterBootstrapStrategy {
    NoQuorumClusterBootstrapStrategy DO_NOT_HANDLE = unused -> ClusterTopologyResult.noQuorum();

    /**
     * Preconditions:
     * - The cluster does not have a quorum of Cassandra servers reachable, but also does not have dissent:
     * that is, all nodes that successfully responded to getHostIds() returned this set of host IDs.
     * - At least one Cassandra server successfully responded to getHostIds().
     */
    ClusterTopologyResult accept(Map<CassandraServer, HostIdResult> hostIdResults);
}

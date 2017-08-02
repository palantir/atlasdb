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

package com.palantir.timelock.partition;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A TimeLockPartitioner constructs mappings from sets of nodes to sets of clients.
 */
public interface TimeLockPartitioner {
    /**
     * This function should return a mapping from hosts to multiple clients, such that for each client, the cluster
     * returned is at least the preferred mini-cluster size.
     */
    Assignment partition(List<String> clients, List<String> hosts, long seed);

    default Set<String> clientsForHost(List<String> clients, List<String> hosts, long seed, String host) {
        return partition(clients, hosts, seed).getClientsForHost(host);
    }

    /**
     * This function uses information about the heterogeneity of clients to more intelligently derive a partition
     * that would utilise resources.
     * Default implementation doesn't use the weights in any way.
     */
    default Assignment weightedPartition(List<String> clients, List<String> hosts, long seed,
            Map<String, Double> clientToWeight) {
        return partition(clients, hosts, seed);
    }
}

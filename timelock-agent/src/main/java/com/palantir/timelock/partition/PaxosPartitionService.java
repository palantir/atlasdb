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

import com.palantir.timelock.coordination.CoordinationService;
import com.palantir.timelock.coordination.DrainService;

public class PaxosPartitionService implements PartitionService {
    private final CoordinationService coordinationService;
    private final Map<String, DrainService> drainServices;
    private final TimeLockPartitioner partitioner;

    private final List<String> clients;
    private final List<String> hosts;

    public PaxosPartitionService(CoordinationService coordinationService,
            Map<String, DrainService> drainServices,
            TimeLockPartitioner partitioner,
            List<String> clients,
            List<String> hosts) {
        this.coordinationService = coordinationService;
        this.drainServices = drainServices;
        this.partitioner = partitioner;
        this.clients = clients;
        this.hosts = hosts;
    }

    @Override
    public void repartition() {
        // TODO (jkong): Fix issues if this crashes mid-way.
        // Currently if this crashes halfway, we need to restart the entire timelock cluster.
        // TODO (jkong): Find a way to throw MultipleRunningCoordinationServiceError if this goes bad?
        Assignment currentAssignment = coordinationService.getCoordinatedValue().assignment();
        Assignment newAssignment = coordinationService.proposeAssignment(
                partitioner.partition(clients, hosts, coordinationService.getSeed())).assignment();

        for (String client : currentAssignment.getKnownClients()) {
            Set<String> currentHosts = currentAssignment.getHostsForClient(client);
            Set<String> newHosts = newAssignment.getHostsForClient(client);

            if (!currentHosts.equals(newHosts)) {
                currentHosts.forEach(host -> drainServices.get(host).drain(client));
                newHosts.forEach(host -> drainServices.get(host).regenerate(client));
            }
        }
    }
}

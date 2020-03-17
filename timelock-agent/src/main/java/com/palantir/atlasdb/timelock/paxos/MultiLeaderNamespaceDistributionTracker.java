/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.paxos;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.immutables.value.Value;

import com.palantir.timelock.paxos.NamespaceTracker;

public final class MultiLeaderNamespaceDistributionTracker {
    private final NamespaceTracker namespaceTracker;
    private final BatchPingableLeader localBatchPingableLeader;

    public MultiLeaderNamespaceDistributionTracker(
            NamespaceTracker namespaceTracker,
            BatchPingableLeader localBatchPingableLeader) {
        this.namespaceTracker = namespaceTracker;
        this.localBatchPingableLeader = localBatchPingableLeader;
    }

    public NamespaceDistribution getCurrentNodeNamespaceDistribution() {
        Set<Client> trackedNamespaces = namespaceTracker.trackedNamespaces();
        List<Client> clientsLedByThisNode = localBatchPingableLeader.ping(trackedNamespaces).stream()
                .sorted(Comparator.comparing(Client::value))
                .collect(Collectors.toList());
        return ImmutableNamespaceDistribution.builder()
                .addAllNamespacesLed(clientsLedByThisNode)
                .numberOfTrackedNamespaces(trackedNamespaces.size())
                .build();
    }

    @Value.Immutable
    public interface NamespaceDistribution {
        List<Client> namespacesLed();
        int numberOfTrackedNamespaces();
    }
}

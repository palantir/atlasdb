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

package com.palantir.timelock.paxos;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.paxos.PaxosResource;

import io.reactivex.Observable;

public class PaxosClientChangeManager {

    private final Observable<Set<String>> paxosClients;
    private final PaxosResource paxosResource;
    private final Function<String, TimeLockServices> timeLockServicesCreator;

    private volatile ImmutableMap<String, TimeLockServices> clientToServices;

    public PaxosClientChangeManager(Observable<Set<String>> paxosClients, PaxosResource paxosResource,
            Function<String, TimeLockServices> timeLockServicesCreator) {
        this.paxosClients = paxosClients;
        this.paxosResource = paxosResource;
        this.timeLockServicesCreator = timeLockServicesCreator;
        this.clientToServices = ImmutableMap.of();
    }

    public void beginWatching() {
        paxosClients.subscribe(newClientSet -> {
            Set<String> existingClients = paxosResource.clientSet();
            ImmutableMap.Builder<String, TimeLockServices> builder = ImmutableMap.builder();

            // Need copy, otherwise the computation for Retained below is wrong (Sets.difference returns a view)
            Set<String> clientsToAdd = ImmutableSet.copyOf(Sets.difference(newClientSet, existingClients));
            for (String client : clientsToAdd) {
                paxosResource.addInstrumentedClient(client);
                builder.put(client, timeLockServicesCreator.apply(client));
            }

            Set<String> clientsToRemove = Sets.difference(existingClients, newClientSet);
            for (String client : clientsToRemove) {
                paxosResource.removeClient(client);
            }

            Set<String> clientsRetained = Sets.difference(existingClients, clientsToAdd);
            for (String client : clientsRetained) {
                builder.put(client, clientToServices.get(client));
            }

            clientToServices = builder.build();
        });
    }

    public Map<String, TimeLockServices> getTimeLockServicesMap() {
        return clientToServices;
    }
}

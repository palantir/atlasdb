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

package com.palantir.atlasdb.timelock.management;

import java.util.Comparator;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.TimelockNamespaces;
import com.palantir.atlasdb.timelock.paxos.PaxosUseCase;
import com.palantir.common.proxy.PredicateSwitchedProxy;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.lock.TimelockNamespace;
import com.palantir.paxos.PaxosAcceptorNetworkClient;
import com.palantir.paxos.PaxosLearnerNetworkClient;
import com.palantir.paxos.PaxosLong;
import com.palantir.paxos.PaxosResponse;
import com.palantir.paxos.PaxosResponses;
import com.palantir.paxos.PaxosUpdate;

public class NamespacedConsensus {

    public static String achieveConsensusForNamespace(TimelockNamespaces timelockNamespaces,
            String namespace) {
        fastForwardTimestampByOneMillion(timelockNamespaces, namespace);
        return "ok";
    }

    private static void fastForwardTimestampByOneMillion(TimelockNamespaces timelockNamespaces, String namespace) {
        TimeLockServices timeLockServices = timelockNamespaces.get(namespace);
        Long timestamp = timeLockServices.getTimelockService().getFreshTimestamp() + 1000000L;
        timeLockServices.getTimestampManagementService().fastForwardTimestamp(timestamp);
    }

    public static String consensusForNamespace(String namespace,
            TimelockNamespaces timelockNamespaces,
            Function<String, PaxosAcceptorNetworkClient> acceptorNetworkClientFactory) {
        while(true) {
            long l1 = getHighestAcceptedValueForNamespace(namespace, acceptorNetworkClientFactory);
            fastForwardTimestampByOneMillion(timelockNamespaces, namespace);
            long l2 = getHighestAcceptedValueForNamespace(namespace, acceptorNetworkClientFactory);
            if (l1 < l2) {
                return "ok";
            }
        }
        // probably should fail after some number of attempts to avoid infinite loop
    }

    private static long getHighestAcceptedValueForNamespace(String namespace,
            Function<String, PaxosAcceptorNetworkClient> acceptorNetworkClientFactory) {
        PaxosResponses<PaxosLong> responses = acceptorNetworkClientFactory.apply(namespace)
                .getLatestSequencePreparedOrAccepted();
        if (!responses.hasQuorum()) {
            throw new ServiceNotAvailableException("No quorum");
        }
        // There is a quorum
        return responses.stream()
                .filter(PaxosLong::isSuccessful).map(PaxosLong::getValue).max(Comparator.naturalOrder()).get();
    }
}

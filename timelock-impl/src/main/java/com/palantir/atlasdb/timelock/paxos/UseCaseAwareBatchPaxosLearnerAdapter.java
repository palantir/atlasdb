/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.collect.SetMultimap;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosValue;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class UseCaseAwareBatchPaxosLearnerAdapter implements BatchPaxosLearner {

    private final PaxosUseCase useCase;
    private final BatchPaxosLearnerRpcClient rpcClient;

    private UseCaseAwareBatchPaxosLearnerAdapter(PaxosUseCase useCase, BatchPaxosLearnerRpcClient rpcClient) {
        this.useCase = useCase;
        this.rpcClient = rpcClient;
    }

    public void learn(SetMultimap<Client, PaxosValue> paxosValuesByClient) {
        rpcClient.learn(useCase, paxosValuesByClient);
    }

    public SetMultimap<Client, PaxosValue> getLearnedValues(Set<WithSeq<Client>> clientAndSeqs) {
        return rpcClient.getLearnedValues(useCase, clientAndSeqs);
    }

    public SetMultimap<Client, PaxosValue> getLearnedValuesSince(Map<Client, Long> seqLowerBoundsByClient) {
        return rpcClient.getLearnedValuesSince(useCase, seqLowerBoundsByClient);
    }

    public static List<BatchPaxosLearner> wrap(PaxosUseCase useCase, List<BatchPaxosLearnerRpcClient> remotes) {
        return remotes.stream()
                .map(rpcClient -> new UseCaseAwareBatchPaxosLearnerAdapter(useCase, rpcClient))
                .collect(Collectors.toList());
    }
}

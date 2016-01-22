/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.transaction.service;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.net.ssl.SSLSocketFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.spi.TransactionServiceConfig;
import com.palantir.atlasdb.transaction.config.PaxosTransactionServiceConfig;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLogManager;
import com.palantir.paxos.PaxosManyLogApi;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosProposerImpl;
import com.palantir.paxos.learner.EmptyPaxosLearner;

public class TransactionServices {
    public static TransactionService createTransactionService(Optional<TransactionServiceConfig> config, KeyValueService keyValueService) {
        if (config.isPresent() && config.get() instanceof PaxosTransactionServiceConfig) {
            final PaxosTransactionServiceConfig paxosConfig = (PaxosTransactionServiceConfig) config.get();
            List<PaxosManyLogApi> endpoints = AtlasDbHttpClients.createRemoteProxies(Optional.<SSLSocketFactory>absent(), paxosConfig.getEndpoints(), PaxosManyLogApi.class);
            List<PaxosAcceptor> acceptors = Lists.transform(endpoints, new Function<PaxosManyLogApi, PaxosAcceptor>() {
                public PaxosAcceptor apply(PaxosManyLogApi input) {
                    return new PaxosLogManager(input).getAcceptor(paxosConfig.getLogName());
                }
            });
            List<PaxosLearner> learners = Lists.transform(endpoints, new Function<PaxosManyLogApi, PaxosLearner>() {
                public PaxosLearner apply(PaxosManyLogApi input) {
                    return new PaxosLogManager(input).getLearner(paxosConfig.getLogName());
                }
            });

            ExecutorService executor = Executors.newCachedThreadPool();
            PaxosProposer proposer = PaxosProposerImpl.newProposer(new EmptyPaxosLearner(), acceptors, learners, paxosConfig.getQuorumSize(), executor);
            return PaxosTransactionService.create(proposer, new TransactionKVSWrapper(keyValueService));
        }

        return new KVSBasedTransactionService(keyValueService);
    }
}

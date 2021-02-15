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
package com.palantir.paxos;

import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;

public class PaxosLatestRoundVerifierImpl implements PaxosLatestRoundVerifier {

    private final PaxosAcceptorNetworkClient acceptorClient;

    public PaxosLatestRoundVerifierImpl(PaxosAcceptorNetworkClient acceptorClient) {
        this.acceptorClient = acceptorClient;
    }

    @Override
    public PaxosQuorumStatus isLatestRound(long round) {
        return AtlasFutures.getUnchecked(isLatestRoundAsync(round));
    }

    @Override
    public ListenableFuture<PaxosQuorumStatus> isLatestRoundAsync(long round) {
        return Futures.transform(
                collectResponses(round), PaxosResponses::getQuorumResult, MoreExecutors.directExecutor());
    }

    private ListenableFuture<PaxosResponses<PaxosResponse>> collectResponses(long round) {
        return FluentFuture.from(acceptorClient.getLatestSequencePreparedOrAcceptedAsync())
                .transform(
                        responses -> responses.map(paxosLong -> acceptorAgreesIsLatestRound(paxosLong, round)),
                        MoreExecutors.directExecutor());
    }

    private static BooleanPaxosResponse acceptorAgreesIsLatestRound(
            PaxosLong latestRoundFromAcceptor, long roundInQuestion) {
        return new BooleanPaxosResponse(roundInQuestion >= latestRoundFromAcceptor.getValue());
    }
}

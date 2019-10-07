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

public class PaxosLatestRoundVerifierImpl implements PaxosLatestRoundVerifier {

    private final PaxosAcceptorNetworkClient acceptorClient;

    public PaxosLatestRoundVerifierImpl(PaxosAcceptorNetworkClient acceptorClient) {
        this.acceptorClient = acceptorClient;
    }

    @Override
    public PaxosQuorumStatus isLatestRound(long round) {
        return collectResponses(round).getQuorumResult();
    }

    private PaxosResponses<PaxosResponse> collectResponses(long round) {
        return acceptorClient.getLatestSequencePreparedOrAccepted()
                .map(paxosLong -> acceptorAgreesIsLatestRound(paxosLong, round));
    }

    private static BooleanPaxosResponse acceptorAgreesIsLatestRound(
            PaxosLong latestRoundFromAcceptor,
            long roundInQuestion) {
        return new BooleanPaxosResponse(roundInQuestion >= latestRoundFromAcceptor.getValue());
    }

}

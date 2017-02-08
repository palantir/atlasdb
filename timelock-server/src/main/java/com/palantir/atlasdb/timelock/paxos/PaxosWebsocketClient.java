/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.atlasdb.timelock.paxos;

import static com.palantir.atlasdb.timelock.paxos.WebsocketMessage.mapUnchecked;

import java.net.URI;
import java.util.Collection;

import com.fasterxml.jackson.core.type.TypeReference;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosPromise;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;
import com.palantir.paxos.PaxosValue;

import io.dropwizard.lifecycle.Managed;

public class PaxosWebsocketClient extends WebsocketClientBase implements PaxosLearner, PaxosAcceptor, Managed  {

    public PaxosWebsocketClient(String serverURI) {
        super(URI.create(serverURI));
    }

    @Override
    public PaxosPromise prepare(long seq, PaxosProposalId pid) {
        WebsocketMessage response = sendRequest(PaxosMethod.PREPARE, Long.toString(seq), mapUnchecked(pid));
        return response.objAt(0, PaxosPromise.class);
    }

    @Override
    public BooleanPaxosResponse accept(long seq, PaxosProposal proposal) {
        WebsocketMessage response = sendRequest(PaxosMethod.ACCEPT, Long.toString(seq), mapUnchecked(proposal));
        return response.objAt(0, BooleanPaxosResponse.class);
    }

    @Override
    public long getLatestSequencePreparedOrAccepted() {
        WebsocketMessage response = sendRequest(PaxosMethod.GET_LATEST_PREPARED_OR_ACCEPTED);
        return response.longAt(0);
    }

    @Override
    public void learn(long seq, PaxosValue val) {
        WebsocketMessage response = sendRequest(PaxosMethod.LEARN, Long.toString(seq), mapUnchecked(val));
        assert response != null;
    }

    @Override
    public PaxosValue getLearnedValue(long seq) {
        WebsocketMessage response = sendRequest(PaxosMethod.GET_LEARNED_VALUE, Long.toString(seq));
        return response.objAt(0, PaxosValue.class);
    }

    @Override
    public PaxosValue getGreatestLearnedValue() {
        WebsocketMessage response = sendRequest(PaxosMethod.GET_GREATEST_LEARNED_VALUE);
        return response.objAt(0, PaxosValue.class);
    }

    @Override
    public Collection<PaxosValue> getLearnedValuesSince(long seq) {
        WebsocketMessage response = sendRequest(PaxosMethod.GET_LEADED_VALUES_SINCE, Long.toString(seq));
        return response.objAt(0, new TypeReference<Collection<PaxosValue>>() {});
    }

}

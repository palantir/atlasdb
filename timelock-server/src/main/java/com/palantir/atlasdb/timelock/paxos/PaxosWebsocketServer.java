/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.atlasdb.timelock.paxos;

import static com.palantir.atlasdb.timelock.paxos.WebsocketMessage.mapUnchecked;

import java.util.Collection;

import org.java_websocket.WebSocket;

import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosPromise;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;
import com.palantir.paxos.PaxosValue;

public class PaxosWebsocketServer extends WebsocketServerBase {

    private final PaxosLearner learner;
    private final PaxosAcceptor acceptor;

    public PaxosWebsocketServer(String uri, PaxosLearner learner, PaxosAcceptor acceptor) {
        super(uri);
        this.learner = learner;
        this.acceptor = acceptor;
    }

    @Override
    public WebsocketMessage processRequest(WebSocket conn, WebsocketMessage message) {
        switch (PaxosMethod.valueOf(message.methodName())) {
            case PREPARE: {
                long seq = message.longAt(0);
                PaxosProposalId pid = message.objAt(1, PaxosProposalId.class);
                PaxosPromise promise = acceptor.prepare(seq, pid);
                return message.response(mapUnchecked(promise));
            }
            case ACCEPT: {
                long seq = message.longAt(0);
                PaxosProposal proposal = message.objAt(1, PaxosProposal.class);
                BooleanPaxosResponse response = acceptor.accept(seq, proposal);
                return message.response(mapUnchecked(response));
            }
            case GET_LATEST_PREPARED_OR_ACCEPTED: {
                Long latest = acceptor.getLatestSequencePreparedOrAccepted();
                return message.response(latest.toString());
            }
            case LEARN: {
                long seq = message.longAt(0);
                PaxosValue value = message.objAt(1, PaxosValue.class);
                learner.learn(seq, value);
                return null;
            }
            case GET_LEARNED_VALUE: {
                long seq = message.longAt(0);
                PaxosValue value = learner.getLearnedValue(seq);
                return message.response(mapUnchecked(value));
            }
            case GET_GREATEST_LEARNED_VALUE: {
                PaxosValue value = learner.getGreatestLearnedValue();
                return message.response(mapUnchecked(value));
            }
            case GET_LEADED_VALUES_SINCE: {
                long seq = message.longAt(0);
                Collection<PaxosValue> values = learner.getLearnedValuesSince(seq);
                return message.response(mapUnchecked(values));
            }
        }

        throw new IllegalArgumentException("unknown method: " + message.methodName());
    }
}

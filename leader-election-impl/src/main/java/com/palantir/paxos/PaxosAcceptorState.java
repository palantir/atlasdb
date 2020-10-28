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

import com.google.common.base.Defaults;
import com.google.protobuf.InvalidProtocolBufferException;
import com.palantir.common.annotation.Immutable;
import com.palantir.common.base.Throwables;
import com.palantir.common.persist.Persistable;
import com.palantir.paxos.persistence.generated.PaxosPersistence;
import java.util.Objects;

/**
 * The logged state (per round) for a paxos acceptor.
 */
@Immutable
public final class PaxosAcceptorState implements Persistable, Versionable {
    final PaxosProposalId lastPromisedId; // latest promised id
    final PaxosProposalId lastAcceptedId; // latest accepted id
    final PaxosValue lastAcceptedValue; // latest accepted value, null if no accepted value
    final long version;

    public static final Hydrator<PaxosAcceptorState> BYTES_HYDRATOR = input -> {
        try {
            PaxosPersistence.PaxosAcceptorState message = PaxosPersistence.PaxosAcceptorState.parseFrom(input);
            return hydrateFromProto(message);
        } catch (InvalidProtocolBufferException e) {
            throw Throwables.throwUncheckedException(e);
        }
    };

    public static PaxosAcceptorState newState(PaxosProposalId pid) {
        return new PaxosAcceptorState(pid);
    }

    private PaxosAcceptorState(PaxosProposalId pid) {
        this.lastPromisedId = pid;
        this.lastAcceptedId = null;
        this.lastAcceptedValue = null;
        this.version = Defaults.defaultValue(long.class);
    }

    private PaxosAcceptorState(PaxosProposalId pid, PaxosProposalId aid, PaxosValue val, long version) {
        this.lastPromisedId = pid;
        this.lastAcceptedId = aid;
        this.lastAcceptedValue = val;
        this.version = version;
    }

    public PaxosAcceptorState withPromise(PaxosProposalId pid) {
        return new PaxosAcceptorState(pid, lastAcceptedId, lastAcceptedValue, version + 1);
    }

    public PaxosAcceptorState withState(PaxosProposalId pid, PaxosProposalId aid, PaxosValue val) {
        return new PaxosAcceptorState(pid, aid, val, version + 1);
    }

    @Override
    public byte[] persistToBytes() {
        PaxosPersistence.PaxosAcceptorState.Builder builder = PaxosPersistence.PaxosAcceptorState.newBuilder();
        if (lastPromisedId != null) {
            builder.setLastPromisedId(lastPromisedId.persistToProto());
        }
        if (lastAcceptedId != null) {
            builder.setLastAcceptedId(lastAcceptedId.persistToProto())
                    .setLastAcceptedValue(lastAcceptedValue.persistToProto());
        }
        return builder.build().toByteArray();
    }

    public static PaxosAcceptorState hydrateFromProto(PaxosPersistence.PaxosAcceptorState message) {
        PaxosProposalId pid = null;
        if (message.hasLastPromisedId()) {
            pid = PaxosProposalId.hydrateFromProto(message.getLastPromisedId());
        }
        PaxosProposalId aid = null;
        if (message.hasLastAcceptedId()) {
            aid = PaxosProposalId.hydrateFromProto(message.getLastAcceptedId());
        }
        PaxosValue val = null;
        if (message.hasLastAcceptedValue()) {
            val = PaxosValue.hydrateFromProto(message.getLastAcceptedValue());
        }
        return new PaxosAcceptorState(pid, aid, val, Defaults.defaultValue(long.class));
    }

    @Override
    public long getVersion() {
        return version;
    }

    public PaxosProposalId getLastPromisedId() {
        return lastPromisedId;
    }

    public PaxosProposalId getLastAcceptedId() {
        return lastAcceptedId;
    }

    public PaxosValue getLastAcceptedValue() {
        return lastAcceptedValue;
    }

    @Override
    public boolean equalsIgnoringVersion(Versionable obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        PaxosAcceptorState other = (PaxosAcceptorState) obj;
        if (!Objects.equals(lastPromisedId, other.lastPromisedId)) {
            return false;
        }
        if (!Objects.equals(lastAcceptedId, other.lastAcceptedId)) {
            return false;
        }
        return Objects.equals(lastAcceptedValue, other.lastAcceptedValue);
    }

    @Override
    public String toString() {
        return "PaxosAcceptorState ["
                + "lastPromisedId="
                + lastPromisedId
                + ", lastAcceptedId="
                + lastAcceptedId
                + ", lastAcceptedValue="
                + lastAcceptedValue
                + "]";
    }
}

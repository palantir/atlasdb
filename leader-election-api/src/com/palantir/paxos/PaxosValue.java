// Copyright 2015 Palantir Technologies
//
// Licensed under the BSD-3 License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://opensource.org/licenses/BSD-3-Clause
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.paxos;

import java.io.Serializable;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Defaults;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.palantir.common.annotation.Immutable;
import com.palantir.common.base.Throwables;
import com.palantir.common.persist.Persistable;
import com.palantir.paxos.persistence.generated.PaxosPersistence;

@Immutable
public class PaxosValue implements Persistable, Versionable, Serializable {
    private static final long serialVersionUID = 1L;

    @Nullable
    final byte[] data;
    final String leaderUUID;
    final long seq;

    public static final Hydrator<PaxosValue> BYTES_HYDRATOR = new Hydrator<PaxosValue>() {
        @Override
        public PaxosValue hydrateFromBytes(byte[] input) {
            try {
                PaxosPersistence.PaxosValue message = PaxosPersistence.PaxosValue.parseFrom(input);
                return hydrateFromProto(message);
            } catch (InvalidProtocolBufferException e) {
                throw Throwables.throwUncheckedException(e);
            }
        }
    };

    public PaxosValue(@JsonProperty("leaderUUID") String leaderUUID,
                      @JsonProperty("seq") long seq,
                      @JsonProperty("data") @Nullable byte[] data) {
        this.leaderUUID = Preconditions.checkNotNull(leaderUUID);
        this.seq = seq;
        this.data = data;
    }

    public String getLeaderUUID() {
        return leaderUUID;
    }

    public long getRound() {
        return seq;
    }

    public byte[] getData() {
        return data;
    }

    public PaxosPersistence.PaxosValue persistToProto() {
        PaxosPersistence.PaxosValue.Builder b = PaxosPersistence.PaxosValue.newBuilder();
        b.setLeaderUUID(leaderUUID).setSeq(seq);
        if (data != null) {
            b.setBytes(ByteString.copyFrom(data));
        }
        return b.build();
    }

    public static PaxosValue hydrateFromProto(PaxosPersistence.PaxosValue message) {
        String leaderUUID = "";
        if (message.hasLeaderUUID()) {
            leaderUUID = message.getLeaderUUID();
        }
        long seq = Defaults.defaultValue(long.class);
        if (message.hasSeq()) {
            seq = message.getSeq();
        }
        byte[] bytes = null;
        if (message.hasBytes()) {
            bytes = message.getBytes().toByteArray();
        }
        return new PaxosValue(leaderUUID, seq, bytes);
    }

    @Override
    public byte[] persistToBytes() {
        return persistToProto().toByteArray();
    }

    @Override
    public long getVersion() {
        return 0;
    }
}

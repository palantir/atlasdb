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
package com.palantir.paxos;

import java.io.Serializable;
import java.util.Arrays;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Defaults;
import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
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
                      @JsonProperty("round") long round,
                      @JsonProperty("data") @Nullable byte[] data) {
        this.leaderUUID = Preconditions.checkNotNull(leaderUUID);
        this.seq = round;
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
    @JsonIgnore
    public long getVersion() {
        return 0;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(data);
        result = prime * result
                + ((leaderUUID == null) ? 0 : leaderUUID.hashCode());
        result = prime * result + (int) (seq ^ (seq >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        PaxosValue other = (PaxosValue) obj;
        if (!Arrays.equals(data, other.data)) {
            return false;
        }
        if (leaderUUID == null) {
            if (other.leaderUUID != null) {
                return false;
            }
        } else if (!leaderUUID.equals(other.leaderUUID)) {
            return false;
        }
        if (seq != other.seq) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "PaxosValue{"
                + "data=" + (data == null ? "null" : BaseEncoding.base16().encode(data))
                + ", leaderUUID='" + leaderUUID + '\''
                + ", seq=" + seq
                + '}';
    }

}

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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Defaults;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.palantir.common.annotation.Immutable;
import com.palantir.common.base.Throwables;
import com.palantir.common.persist.Persistable;
import com.palantir.logsafe.Preconditions;
import com.palantir.paxos.persistence.generated.PaxosPersistence;
import java.io.Serializable;
import java.util.Arrays;
import javax.annotation.Nullable;

@Immutable
public class PaxosValue implements Persistable, Versionable, Serializable {
    private static final long serialVersionUID = 1L;

    @Nullable
    final byte[] data;
    final String leaderUuid;
    final long seq;

    public static final Hydrator<PaxosValue> BYTES_HYDRATOR = input -> {
        try {
            PaxosPersistence.PaxosValue message = PaxosPersistence.PaxosValue.parseFrom(input);
            return hydrateFromProto(message);
        } catch (InvalidProtocolBufferException e) {
            throw Throwables.throwUncheckedException(e);
        }
    };

    public PaxosValue(@JsonProperty("leaderUUID") String leaderUuid,
                      @JsonProperty("round") long round,
                      @JsonProperty("data") @Nullable byte[] data) {
        this.leaderUuid = Preconditions.checkNotNull(leaderUuid, "leaderUUID should never be null");
        this.seq = round;
        this.data = data;
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName") // Avoiding API break
    public String getLeaderUUID() {
        return leaderUuid;
    }

    public long getRound() {
        return seq;
    }

    public byte[] getData() {
        return data;
    }

    public PaxosPersistence.PaxosValue persistToProto() {
        PaxosPersistence.PaxosValue.Builder builder = PaxosPersistence.PaxosValue.newBuilder();
        builder.setLeaderUUID(leaderUuid).setSeq(seq);
        if (data != null) {
            builder.setBytes(ByteString.copyFrom(data));
        }
        return builder.build();
    }

    public static PaxosValue hydrateFromProto(PaxosPersistence.PaxosValue message) {
        String leaderUuid = "";
        if (message.hasLeaderUUID()) {
            leaderUuid = message.getLeaderUUID();
        }
        long seq = Defaults.defaultValue(long.class);
        if (message.hasSeq()) {
            seq = message.getSeq();
        }
        byte[] bytes = null;
        if (message.hasBytes()) {
            bytes = message.getBytes().toByteArray();
        }
        return new PaxosValue(leaderUuid, seq, bytes);
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
                + ((leaderUuid == null) ? 0 : leaderUuid.hashCode());
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
        if (leaderUuid == null) {
            if (other.leaderUuid != null) {
                return false;
            }
        } else if (!leaderUuid.equals(other.leaderUuid)) {
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
                + ", leaderUUID='" + leaderUuid + '\''
                + ", seq=" + seq
                + '}';
    }

}

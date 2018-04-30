/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.palantir.common.annotation.Immutable;
import com.palantir.paxos.persistence.generated.remoting.PaxosAcceptorPersistence;

/**
 * A proposal of a value that needs quorum support.
 *
 * @author rullman
 */
@Immutable
public class PaxosProposal implements Serializable {
    private static final long serialVersionUID = 1L;

    final PaxosProposalId id;
    @Nonnull
    final PaxosValue val;

    @JsonCreator
    public PaxosProposal(@JsonProperty("id") PaxosProposalId id,
                         @JsonProperty("value") PaxosValue val) {
        this.id = id;
        this.val = Preconditions.checkNotNull(val, "value cannot be null");
    }

    public PaxosProposalId getId() {
        return id;
    }

    public PaxosValue getValue() {
        return val;
    }

    public static PaxosProposal hydrateFromProto(PaxosAcceptorPersistence.PaxosProposal parseFrom) {
        PaxosProposalId id = PaxosProposalId.hydrateFromProto(parseFrom.getId());
        PaxosValue value = PaxosValue.hydrateFromProto(parseFrom.getVal());
        return new PaxosProposal(id, value);
    }

    public PaxosAcceptorPersistence.PaxosProposal persistToProto() {
        return PaxosAcceptorPersistence.PaxosProposal.newBuilder()
                .setId(id.persistToProto())
                .setVal(val.persistToProto())
                .build();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((val == null) ? 0 : val.hashCode());
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
        PaxosProposal other = (PaxosProposal) obj;
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!id.equals(other.id)) {
            return false;
        }
        if (val == null) {
            if (other.val != null) {
                return false;
            }
        } else if (!val.equals(other.val)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "PaxosProposal [id=" + id + ", val=" + val + "]";
    }
}

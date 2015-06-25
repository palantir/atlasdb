// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.paxos;

import java.io.Serializable;

import javax.annotation.Nonnull;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.palantir.common.annotation.Immutable;
import com.palantir.paxos.persistence.generated.PaxosPersistence;

/**
 * Proposal identifiers that establish a strict ordering.
 *
 * @author rullman
 */
@Immutable
public class PaxosProposalId implements Comparable<PaxosProposalId>, Serializable {
    private static final long serialVersionUID = 1L;

    private final long number;
    @Nonnull
    private final String proposerUUID;

    public PaxosProposalId(@JsonProperty("number") long number,
                           @JsonProperty("proposerUUID") String proposerUUID) {
        this.number = number;
        this.proposerUUID = Preconditions.checkNotNull(proposerUUID);
    }

    @Override
    public int compareTo(PaxosProposalId o) {
        return new CompareToBuilder()
            .append(getNumber(), o.getNumber())
            .append(getProposerUUID(), o.getProposerUUID())
            .toComparison();
    }

    public PaxosPersistence.PaxosProposalId persistToProto() {
        return PaxosPersistence.PaxosProposalId.newBuilder()
                .setNumber(getNumber())
                .setProposerUUID(getProposerUUID())
                .build();
    }

    public static PaxosProposalId hydrateFromProto(PaxosPersistence.PaxosProposalId message) {
        long number = message.getNumber();
        String proposerUUID = "";
        if (message.hasProposerUUID()) {
            proposerUUID = message.getProposerUUID();
        }
        return new PaxosProposalId(number, proposerUUID);
    }

    public long getNumber() {
        return number;
    }

    public String getProposerUUID() {
        return proposerUUID;
    }
}

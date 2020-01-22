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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.palantir.common.annotation.Immutable;
import com.palantir.logsafe.Preconditions;
import com.palantir.paxos.persistence.generated.PaxosPersistence;
import java.io.Serializable;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.builder.CompareToBuilder;

/**
 * Proposal identifiers that establish a strict ordering.
 *
 * @author rullman
 */
@Immutable
public class PaxosProposalId implements Comparable<PaxosProposalId>, Serializable {
    private static final long serialVersionUID = 1L;

    final long number;
    @Nonnull
    private final String proposerUuid;

    public PaxosProposalId(@JsonProperty("number") long number,
                           @JsonProperty("proposerUUID") String proposerUuid) {
        this.number = number;
        this.proposerUuid = Preconditions.checkNotNull(proposerUuid, "proposerUUID cannot be null");
    }

    @Override
    public int compareTo(PaxosProposalId paxosProposalId) {
        return new CompareToBuilder()
            .append(getNumber(), paxosProposalId.getNumber())
            .append(getProposerUUID(), paxosProposalId.getProposerUUID())
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
        String proposerUuid = "";
        if (message.hasProposerUUID()) {
            proposerUuid = message.getProposerUUID();
        }
        return new PaxosProposalId(number, proposerUuid);
    }

    public long getNumber() {
        return number;
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName") // Avoiding API break
    public String getProposerUUID() {
        return proposerUuid;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (number ^ (number >>> 32));
        result = prime * result
                + ((proposerUuid == null) ? 0 : proposerUuid.hashCode());
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
        PaxosProposalId other = (PaxosProposalId) obj;
        if (number != other.number) {
            return false;
        }
        if (proposerUuid == null) {
            if (other.proposerUuid != null) {
                return false;
            }
        } else if (!proposerUuid.equals(other.proposerUuid)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "PaxosProposalId [number=" + number + ", proposerUUID="
                + proposerUuid + "]";
    }
}

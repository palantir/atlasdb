/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.paxos;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.palantir.common.annotation.Immutable;

/**
 * A promise to not accept new proposals less than promisedID.
 *
 * @author rullman
 */
@Immutable
public class PaxosPromise implements Comparable<PaxosPromise>, PaxosResponse {
    private static final long serialVersionUID = 1L;

    final boolean ack;
    @Nonnull final PaxosProposalId promisedId;
    @Nullable final PaxosProposalId lastAcceptedId;
    @Nullable final PaxosValue lastAcceptedValue;

    public PaxosPromise(PaxosProposalId promisedId) {
        ack = false;
        this.promisedId = Preconditions.checkNotNull(promisedId);
        lastAcceptedId = null;
        lastAcceptedValue = null;
    }

    public PaxosPromise(PaxosProposalId promisedId,
                        PaxosProposalId lastAcceptedId,
                        PaxosValue val) {
        ack = true;
        this.promisedId = Preconditions.checkNotNull(promisedId);
        this.lastAcceptedId = lastAcceptedId;
        this.lastAcceptedValue = val;
    }

    @JsonCreator
    public static PaxosPromise create(@JsonProperty("successful") boolean ack,
                                      @JsonProperty("promisedId") PaxosProposalId promisedId,
                                      @JsonProperty("lastAcceptedId") PaxosProposalId lastAcceptedId,
                                      @JsonProperty("lastAcceptedValue") PaxosValue val) {
        if (ack) {
            return new PaxosPromise(promisedId, lastAcceptedId, val);
        } else {
            return new PaxosPromise(promisedId);
        }
    }

    @Override
    public int compareTo(PaxosPromise o) {
        // nulls are less than non-nulls so nacks are less than acks
        return new CompareToBuilder().append(lastAcceptedId, o.lastAcceptedId).toComparison();
    }

    @Override
    public boolean isSuccessful() {
        return ack;
    }

    public PaxosProposalId getPromisedId() {
        return promisedId;
    }

    public PaxosProposalId getLastAcceptedId() {
        return lastAcceptedId;
    }

    public PaxosValue getLastAcceptedValue() {
        return lastAcceptedValue;
    }
}

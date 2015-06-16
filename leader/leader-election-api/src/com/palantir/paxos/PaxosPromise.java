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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang.builder.CompareToBuilder;

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
        this.promisedId = promisedId;
        lastAcceptedId = null;
        lastAcceptedValue = null;
    }

    public PaxosPromise(PaxosProposalId promisedId,
                        PaxosProposalId lastAcceptedId,
                        PaxosValue val) {
        ack = true;
        this.promisedId = promisedId;
        this.lastAcceptedId = lastAcceptedId;
        this.lastAcceptedValue = val;
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
}

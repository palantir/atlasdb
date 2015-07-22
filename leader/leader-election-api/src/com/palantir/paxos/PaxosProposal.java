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

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.palantir.common.annotation.Immutable;

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

    public PaxosProposal(@JsonProperty("id") PaxosProposalId id,
                         @JsonProperty("value") PaxosValue val) {
        this.id = Preconditions.checkNotNull(id);
        this.val = Preconditions.checkNotNull(val);
    }

    public PaxosProposalId getId() {
        return id;
    }

    public PaxosValue getValue() {
        return val;
    }
}

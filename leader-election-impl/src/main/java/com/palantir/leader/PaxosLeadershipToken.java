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
package com.palantir.leader;

import com.palantir.common.annotation.Immutable;
import com.palantir.leader.LeaderElectionService.LeadershipToken;
import com.palantir.paxos.PaxosValue;
import org.apache.commons.lang3.builder.CompareToBuilder;

@Immutable
public class PaxosLeadershipToken implements LeadershipToken {
    private static final long serialVersionUID = 1L;

    final PaxosValue value;

    public PaxosLeadershipToken(PaxosValue value) {
        this.value = value;
    }

    @Override
    public boolean sameAs(LeadershipToken obj) {
        if ((obj == null) || (obj.getClass() != this.getClass())) {
            return false;
        }
        PaxosValue paxosValue = ((PaxosLeadershipToken) obj).value;
        if (value == paxosValue) {
            return true;
        }
        return value != null && paxosValue != null
                && (new CompareToBuilder().append(value.getLeaderUUID(), paxosValue.getLeaderUUID())
                .append(value.getRound(), paxosValue.getRound())
                .append(value.getData(), paxosValue.getData())
                .toComparison() == 0);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((value == null) ? 0 : value.hashCode());
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
        PaxosLeadershipToken other = (PaxosLeadershipToken) obj;
        if (value == null) {
            if (other.value != null) {
                return false;
            }
        } else if (!value.equals(other.value)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "PaxosLeadershipToken{" + value + '}';
    }
}

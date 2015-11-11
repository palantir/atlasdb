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
package com.palantir.leader;

import javax.annotation.CheckForNull;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.palantir.common.annotation.Immutable;
import com.palantir.leader.LeaderElectionService.LeadershipToken;
import com.palantir.paxos.PaxosValue;

@Immutable
public class PaxosLeadershipToken implements LeadershipToken {
    private static final long serialVersionUID = 1L;

    @CheckForNull final PaxosValue value;

    public PaxosLeadershipToken(@CheckForNull PaxosValue value) {
        this.value = value;
    }

    @Override
    public boolean sameAs(LeadershipToken o) {
        if ((o == null) || (o.getClass() != this.getClass())) {
            return false;
        }
        PaxosValue v = ((PaxosLeadershipToken) o).value;
        if (value == v) {
            return true;
        }
        return value != null && v != null
                && (new CompareToBuilder().append(value.getLeaderUUID(), v.getLeaderUUID())
                                          .append(value.getRound(), v.getRound())
                                          .append(value.getData(), v.getData())
                                          .toComparison() == 0);
    }

    @Override
    public String toString() {
        return "PaxosLeadershipToken{" + value + '}';
    }
}

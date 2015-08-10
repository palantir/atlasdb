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
}

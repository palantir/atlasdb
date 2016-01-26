package com.palantir.paxos.config;

import java.util.Set;

import org.immutables.value.Value;
import org.immutables.value.Value.Check;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;

@JsonDeserialize(as = ImmutablePaxosProposerConfig.class)
@JsonSerialize(as = ImmutablePaxosProposerConfig.class)
@Value.Immutable
public abstract class PaxosProposerConfig {
    public abstract Set<String> getEndpoints();
    public abstract int getQuorumSize();

    @Check
    protected final void check() {
        Preconditions.checkArgument(getQuorumSize() > getEndpoints().size() / 2,
                "Quorum must be a majority.");
    }
}

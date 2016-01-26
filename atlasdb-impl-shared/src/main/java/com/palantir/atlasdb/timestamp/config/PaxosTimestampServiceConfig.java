package com.palantir.atlasdb.timestamp.config;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.service.AutoService;
import com.palantir.atlasdb.spi.TimestampServiceConfig;
import com.palantir.paxos.config.PaxosProposerConfig;

@AutoService(TimestampServiceConfig.class)
@JsonDeserialize(as = ImmutablePaxosTimestampServiceConfig.class)
@JsonSerialize(as = ImmutablePaxosTimestampServiceConfig.class)
@JsonTypeName(PaxosTimestampServiceConfig.TYPE)
@Value.Immutable
public abstract class PaxosTimestampServiceConfig implements TimestampServiceConfig {
    public static final String TYPE = "paxos";

    @Override
    public final String type() {
        return TYPE;
    }

    @Value.Default
    public String getLogName() {
        return "timestamp";
    }

    public abstract PaxosProposerConfig proposer();

}

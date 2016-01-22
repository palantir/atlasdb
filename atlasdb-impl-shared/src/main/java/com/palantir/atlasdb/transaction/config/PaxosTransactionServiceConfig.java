package com.palantir.atlasdb.transaction.config;

import java.util.Set;

import org.immutables.value.Value;
import org.immutables.value.Value.Check;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.TransactionServiceConfig;

@AutoService(KeyValueServiceConfig.class)
@JsonDeserialize(as = ImmutablePaxosTransactionServiceConfig.class)
@JsonSerialize(as = ImmutablePaxosTransactionServiceConfig.class)
@JsonTypeName(PaxosTransactionServiceConfig.TYPE)
@Value.Immutable
public abstract class PaxosTransactionServiceConfig implements TransactionServiceConfig {
    public static final String TYPE = "paxos";

    @Override
    public final String type() {
        return TYPE;
    }

    @Value.Default
    public String getLogName() {
        return "transaction";
    }

    public abstract Set<String> getEndpoints();
    public abstract String localServer();

    public abstract int getQuorumSize();

    @Check
    protected final void check() {
        Preconditions.checkArgument(getEndpoints().contains(localServer()),
                "The localServer '%s' must included in the entries %s.", localServer(), getEndpoints());
        Preconditions.checkArgument(getQuorumSize() > getEndpoints().size() / 2,
                "Quorum must be a majority.");
    }
}

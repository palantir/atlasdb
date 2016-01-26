package com.palantir.atlasdb.keyvalue.partition;

import java.util.Set;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.service.AutoService;
import com.google.common.base.Optional;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;

@AutoService(KeyValueServiceConfig.class)
@JsonTypeName(StaticPartitionedKeyValueConfiguration.TYPE)
@JsonDeserialize(as = ImmutableStaticPartitionedKeyValueConfiguration.class)
@JsonSerialize(as = ImmutableStaticPartitionedKeyValueConfiguration.class)
@Value.Immutable
public abstract class StaticPartitionedKeyValueConfiguration implements KeyValueServiceConfig {
    public static final String TYPE = "static_partitioned";

    @Override
    public final String type() {
        return TYPE;
    }

    public abstract Optional<QuorumParameters> getQuorumParameters();
    public abstract Optional<KeyValueServiceConfig> keyValueService();
    public abstract Set<String> getEndpoints();
}

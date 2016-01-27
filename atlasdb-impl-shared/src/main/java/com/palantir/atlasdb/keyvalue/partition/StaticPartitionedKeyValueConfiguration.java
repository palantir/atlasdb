package com.palantir.atlasdb.keyvalue.partition;

import java.util.List;

import org.immutables.value.Value;
import org.immutables.value.Value.Check;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
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

    public abstract List<String> getKeyValueEndpoints();

    @Check
    protected void check() {
        Preconditions.checkArgument(!getKeyValueEndpoints().isEmpty());
        Preconditions.checkArgument(Sets.newHashSet(getKeyValueEndpoints()).size() == getKeyValueEndpoints().size());
    }
}

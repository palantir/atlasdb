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
package com.palantir.atlasdb.keyvalue.dbkvs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.nexus.db.pool.config.ConnectionConfig;
import java.util.Optional;
import org.immutables.value.Value;

@AutoService(KeyValueServiceConfig.class)
@JsonDeserialize(as = ImmutableDbKeyValueServiceConfig.class)
@JsonSerialize(as = ImmutableDbKeyValueServiceConfig.class)
@JsonTypeName(DbAtlasDbFactory.TYPE)
@Value.Immutable
public abstract class DbKeyValueServiceConfig implements KeyValueServiceConfig {
    public abstract DdlConfig ddl();

    public abstract ConnectionConfig connection();

    @Override
    @JsonIgnore
    @Value.Derived
    public Optional<String> namespace() {
        return connection().namespace();
    }

    @Override
    public final String type() {
        return DbAtlasDbFactory.TYPE;
    }

    @Override
    @Value.Default
    public int concurrentGetRangesThreadPoolSize() {
        return Math.max(2 * connection().getMaxConnections() / 3, 1);
    }

    @Value.Check
    protected final void check() {
        Preconditions.checkArgument(ddl().type().equals(connection().type()),
                "ddl config (%s) and connection config (%s) must be for the same physical store",
                ddl().type(), connection().type());
    }
}

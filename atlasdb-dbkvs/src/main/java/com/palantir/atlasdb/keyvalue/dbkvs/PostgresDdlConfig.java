/*
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
package com.palantir.atlasdb.keyvalue.dbkvs;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.remoting.api.config.service.HumanReadableDuration;

@JsonDeserialize(as = ImmutablePostgresDdlConfig.class)
@JsonSerialize(as = ImmutablePostgresDdlConfig.class)
@JsonTypeName(PostgresDdlConfig.TYPE)
@Value.Immutable
public abstract class PostgresDdlConfig extends DdlConfig {
    public static final String TYPE = "postgres";

    @Value.Default
    @Override
    public TableReference metadataTable() {
        return AtlasDbConstants.DEFAULT_METADATA_TABLE;
    }

    @Override
    public final String type() {
        return TYPE;
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    @Value.Default
    public HumanReadableDuration compactInterval() {
        return HumanReadableDuration.seconds(7);
    }
}

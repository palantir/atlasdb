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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.db.oracle.JdbcHandler;
import com.palantir.db.oracle.NativeOracleJdbcHandler;
import com.palantir.logsafe.Preconditions;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableOracleDdlConfig.class)
@JsonSerialize(as = ImmutableOracleDdlConfig.class)
@JsonTypeName(OracleDdlConfig.TYPE)
@Value.Immutable
@JsonIgnoreProperties("jdbcHandler")
public abstract class OracleDdlConfig extends DdlConfig {
    public static final String TYPE = "oracle";

    /**
     * TODO(jbaker): Refactor away the existence of this class. This was originally split between an external project
     * and an internal project because Oracle did not publish their driver to Maven. So, we managed to split things
     * up so that there was a single interface which was implemented internally which we dynamically loaded.
     * We're merging the two back together, but I'm not going to ramp up on the Oracle jdbc driver as part of this,
     * so view this as something vestigial.
     */
    public final JdbcHandler jdbcHandler() {
        return new NativeOracleJdbcHandler();
    }

    @Value.Default
    public String singleOverflowTable() {
        return "atlas_overflow";
    }

    @Value.Default
    public String overflowTablePrefix() {
        return "ao_";
    }

    @JsonIgnore
    public abstract Optional<Supplier<Long>> overflowIds();

    public abstract OverflowMigrationState overflowMigrationState();

    @Value.Default
    public boolean enableOracleEnterpriseFeatures() {
        return false;
    }

    @Value.Default
    public boolean enableShrinkOnOracleStandardEdition() {
        return false;
    }

    @Value.Default
    public long compactionConnectionTimeout() {
        return Duration.ofHours(10).toMillis();
    }

    @Value.Default
    @Override
    public String tablePrefix() {
        return "a_";
    }

    @Value.Default
    @Override
    public TableReference metadataTable() {
        return AtlasDbConstants.DEFAULT_ORACLE_METADATA_TABLE;
    }

    @Value.Default
    @JsonIgnore
    public boolean useTableMapping() {
        return true;
    }

    @Override
    public final String type() {
        return TYPE;
    }

    @Value.Check
    protected final void checkOracleConfig() {
        Preconditions.checkState(tablePrefix() != null, "Oracle 'tablePrefix' cannot be null.");
        Preconditions.checkState(!tablePrefix().isEmpty(), "Oracle 'tablePrefix' must not be an empty string.");
        Preconditions.checkState(!tablePrefix().startsWith("_"), "Oracle 'tablePrefix' cannot begin with underscore.");
        Preconditions.checkState(tablePrefix().endsWith("_"), "Oracle 'tablePrefix' must end with an underscore.");
        com.google.common.base.Preconditions.checkState(
                tablePrefix().length() <= AtlasDbConstants.MAX_TABLE_PREFIX_LENGTH,
                "Oracle 'tablePrefix' cannot be more than %s characters long.",
                AtlasDbConstants.MAX_TABLE_PREFIX_LENGTH);
        Preconditions.checkState(
                !overflowTablePrefix().startsWith("_"), "Oracle 'overflowTablePrefix' cannot begin with underscore.");
        Preconditions.checkState(
                overflowTablePrefix().endsWith("_"), "Oracle 'overflowTablePrefix' must end with an underscore.");
        com.google.common.base.Preconditions.checkState(
                overflowTablePrefix().length() <= AtlasDbConstants.MAX_OVERFLOW_TABLE_PREFIX_LENGTH,
                "Oracle 'overflowTablePrefix' cannot be more than %s characters long.",
                AtlasDbConstants.MAX_OVERFLOW_TABLE_PREFIX_LENGTH);
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}

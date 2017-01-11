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
package com.palantir.atlasdb.keyvalue.dbkvs;

import java.util.Optional;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbTableFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OracleDbTableFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.db.oracle.JdbcHandler;

@JsonDeserialize(as = ImmutableOracleDdlConfig.class)
@JsonSerialize(as = ImmutableOracleDdlConfig.class)
@JsonTypeName(OracleDdlConfig.TYPE)
@Value.Immutable
public abstract class OracleDdlConfig extends DdlConfig {
    public static final String TYPE = "oracle";

    public abstract JdbcHandler jdbcHandler();

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

    @Override
    public Supplier<DbTableFactory> tableFactorySupplier() {
        return () -> new OracleDbTableFactory(this);
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
        Preconditions.checkState(
                tablePrefix().length() <= AtlasDbConstants.MAX_TABLE_PREFIX_LENGTH,
                "Oracle 'tablePrefix' cannot be more than %s characters long.",
                AtlasDbConstants.MAX_TABLE_PREFIX_LENGTH);
        Preconditions.checkState(
                !overflowTablePrefix().startsWith("_"),
                "Oracle 'overflowTablePrefix' cannot begin with underscore.");
        Preconditions.checkState(
                overflowTablePrefix().endsWith("_"),
                "Oracle 'overflowTablePrefix' must end with an underscore.");
        Preconditions.checkState(
                overflowTablePrefix().length() <= AtlasDbConstants.MAX_OVERFLOW_TABLE_PREFIX_LENGTH,
                "Oracle 'overflowTablePrefix' cannot be more than %s characters long.",
                AtlasDbConstants.MAX_OVERFLOW_TABLE_PREFIX_LENGTH);
    }
}

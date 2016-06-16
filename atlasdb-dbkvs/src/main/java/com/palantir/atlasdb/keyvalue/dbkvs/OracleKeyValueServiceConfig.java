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

import org.immutables.value.Value;

import com.google.common.base.Supplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbTableFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OracleDbTableFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.db.oracle.JdbcHandler;

@Value.Immutable
public abstract class OracleKeyValueServiceConfig extends DbKeyValueServiceConfig {

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

    public abstract Supplier<Long> overflowIds();

    public abstract OverflowMigrationState overflowMigrationState();

    @Value.Default
    public boolean enableOracleEnterpriseFeatures() {
        return false;
    }

    @Override
    public Supplier<DbTableFactory> tableFactorySupplier() {
        return () -> new OracleDbTableFactory(this);
    }

    @Override
    public final String type() {
        return TYPE;
    }

}

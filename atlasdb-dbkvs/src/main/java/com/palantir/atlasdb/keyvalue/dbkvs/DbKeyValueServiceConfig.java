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

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbTableFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.nexus.db.pool.config.ConnectionConfig;

public abstract class DbKeyValueServiceConfig implements KeyValueServiceConfig {

    @Value.Default
    public DbSharedConfig shared() {
        return ImmutableDbSharedConfig.builder().build();
    }

    // must be set if you want to call DbKvs.create()
    // otherwise you must instantiate DbKvs by calling the constructor directly
    public abstract Optional<ConnectionConfig> connection();

    @Value.Derived
    public abstract Supplier<DbTableFactory> tableFactorySupplier();

}

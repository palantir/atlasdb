/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.services;

import java.util.Optional;
import java.util.Set;

import javax.net.ssl.SSLSocketFactory;

import org.immutables.value.Value;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.factory.ServiceDiscoveringAtlasSupplier;
import com.palantir.atlasdb.qos.FakeQosClient;
import com.palantir.atlasdb.table.description.Schema;

@Value.Immutable
public abstract class ServicesConfig {

    public abstract AtlasDbConfig atlasDbConfig();

    public abstract AtlasDbRuntimeConfig atlasDbRuntimeConfig();

    @Value.Derived
    public ServiceDiscoveringAtlasSupplier atlasDbSupplier() {
        return new ServiceDiscoveringAtlasSupplier(
                atlasDbConfig().keyValueService(),
                atlasDbConfig().leader(),
                atlasDbConfig().namespace(),
                atlasDbConfig().initializeAsync(),
                FakeQosClient.INSTANCE);
    }

    @Value.Default
    public Set<Schema> schemas() {
        return ImmutableSet.of();
    }

    @Value.Default
    public boolean allowAccessToHiddenTables() {
        return true;
    }

    public abstract Optional<SSLSocketFactory> sslSocketFactory();

}

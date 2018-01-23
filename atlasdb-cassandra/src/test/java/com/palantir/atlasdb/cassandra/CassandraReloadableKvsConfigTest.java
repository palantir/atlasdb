/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.cassandra;

import static org.mockito.Mockito.mock;

import java.util.Optional;

import org.junit.Test;

public class CassandraReloadableKvsConfigTest {
    private static CassandraKeyValueServiceConfig config = mock(CassandraKeyValueServiceConfig.class);

    private CassandraKeyValueServiceRuntimeConfig runtimeConfig = mock(
            CassandraKeyValueServiceRuntimeConfig.class);

    private CassandraReloadableKvsConfig reloadableConfig = new CassandraReloadableKvsConfig(
            config,
            () -> Optional.of(runtimeConfig));

    @Test
    public void ifNoRuntimeConfig_resolvesToInstallConfig() {
        CassandraReloadableKvsConfig reloadableConfig = new CassandraReloadableKvsConfig(config, Optional::empty);
    }

    @Test
    public void ifInstallAndRuntimeConfig_resolvesToRuntimeConfig() {

    }

    @Test
    public void ifRuntimeConfigIsModified_reloadableConfigIsAlsoModified() {

    }

    private CassandraReloadableKvsConfig getReloadableConfigWithEmptyRuntimeConfig() {
        return new CassandraReloadableKvsConfig(config, Optional::empty);
    }

    private CassandraReloadableKvsConfig getReloadableConfigWithRuntimeConfig(
            CassandraKeyValueServiceRuntimeConfig runtimeConfig) {
        return new CassandraReloadableKvsConfig(config, () -> Optional.of(runtimeConfig));
    }
}

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
package com.palantir.atlasdb.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class CassandraReloadableKvsConfigTest {
    private CassandraKeyValueServiceConfig config;
    private CassandraKeyValueServiceRuntimeConfig runtimeConfig;

    @Before
    public void setUp() {
        config = mock(CassandraKeyValueServiceConfig.class);
        runtimeConfig = mock(CassandraKeyValueServiceRuntimeConfig.class);
    }

    @Test
    public void ifNoRuntimeConfig_resolvesToInstallConfig() {
        CassandraReloadableKvsConfig reloadableConfig = getReloadableConfigWithEmptyRuntimeConfig();

        boolean installConfigParam = true;
        when(config.autoRefreshNodes()).thenReturn(installConfigParam);
        assertThat(reloadableConfig.autoRefreshNodes()).isEqualTo(installConfigParam);
    }

    @Test
    public void ifInstallAndRuntimeConfig_resolvesToRuntimeConfig() {
        CassandraReloadableKvsConfig reloadableConfig = getReloadableConfigWithRuntimeConfig();

        int installConfigParam = 1;
        when(config.sweepReadThreads()).thenReturn(installConfigParam);

        int runtimeConfigParam = 2;
        when(runtimeConfig.sweepReadThreads()).thenReturn(runtimeConfigParam);

        assertThat(reloadableConfig.sweepReadThreads()).isEqualTo(runtimeConfigParam);
    }

    @Test
    public void ifRuntimeConfigIsModified_reloadableConfigIsAlsoModified() {
        CassandraReloadableKvsConfig reloadableConfig = getReloadableConfigWithRuntimeConfig();

        int firstValue = 1;
        int secondValue = 2;
        when(runtimeConfig.sweepReadThreads()).thenReturn(firstValue, secondValue);
        assertThat(reloadableConfig.sweepReadThreads()).isEqualTo(firstValue);
        assertThat(reloadableConfig.sweepReadThreads()).isEqualTo(secondValue);
    }

    private CassandraReloadableKvsConfig getReloadableConfigWithEmptyRuntimeConfig() {
        return new CassandraReloadableKvsConfig(config, Optional::empty);
    }

    private CassandraReloadableKvsConfig getReloadableConfigWithRuntimeConfig() {
        return new CassandraReloadableKvsConfig(config, () -> Optional.of(runtimeConfig));
    }
}

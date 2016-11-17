/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.cli.command.timestamp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbConfigs;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ImmutableTimeLockClientConfig;
import com.palantir.atlasdb.config.TimeLockClientConfig;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.RateLimitedTimestampService;
import com.palantir.timestamp.TimestampService;

public class TimestampServicesProvidersTest {
    private static final String CLIENT = "client";
    private static final String SERVER = "http://localhost:8700";
    private static final TimeLockClientConfig CLIENT_CONFIG = ImmutableTimeLockClientConfig.builder()
            .client(CLIENT)
            .serverListConfig(ImmutableServerListConfig.builder()
                    .addServers(SERVER)
                    .build())
            .build();

    @Test
    public void createsInternalTimestampService() throws IOException {
        AtlasDbConfig config = AtlasDbConfigs.load(new File("src/test/resources/cli_test_config.yml"));
        TimestampServicesProvider provider = TimestampServicesProviders.getInternalProviderFromAtlasDbConfig(config);
        assertThat(provider.timestampService()).isNotNull();
    }

    @Test
    public void createsInternalTimestampServiceEvenWithTimelockBlockPresent() throws IOException {
        AtlasDbConfig config = AtlasDbConfigs.load(new File("src/test/resources/cli_test_config_timelock.yml"));
        TimestampServicesProvider provider = TimestampServicesProviders.getInternalProviderFromAtlasDbConfig(config);
        assertThat(provider.timestampService() instanceof InMemoryTimestampService).isTrue();
    }

    @Test
    public void createsFromSingleService() {
        TimestampService service = new InMemoryTimestampService();
        TimestampServicesProvider provider = TimestampServicesProviders.createFromSingleService(service);
        assertThat(provider.timestampAdministrationService()).isNotNull();
    }

    @Test
    public void cannotCreateFromSingleTimestampServiceWithoutAdminCapabilities() {
        TimestampService service = new RateLimitedTimestampService(new InMemoryTimestampService(), 1000);
        assertThatThrownBy(() -> TimestampServicesProviders.createFromSingleService(service))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void canCreateTimelockProxy() {
        TimestampServicesProvider provider = TimestampServicesProviders.createFromTimelockConfiguration(CLIENT_CONFIG);
        assertThat(provider.timestampService()).isNotNull();
        assertThat(provider.timestampAdministrationService()).isNotNull();
    }
}

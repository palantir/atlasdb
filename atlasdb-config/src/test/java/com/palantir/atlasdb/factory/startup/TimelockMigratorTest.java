/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.factory.startup;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ImmutableTimeLockClientConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.config.TimeLockClientConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.timestamp.TimestampStoreInvalidator;

public class TimelockMigratorTest {
    private static final int PORT = 8080;

    private static final long BACKUP_TIMESTAMP = 42;
    private static final String TEST_ENDPOINT = "/testClient/timestamp-management/fast-forward?currentTimestamp="
            + BACKUP_TIMESTAMP;
    private static final String PING_ENDPOINT = "/testClient/timestamp-management/ping";
    private static final MappingBuilder TEST_MAPPING = post(urlEqualTo(TEST_ENDPOINT));
    private static final MappingBuilder PING_MAPPING = get(urlEqualTo(PING_ENDPOINT));

    private static final KeyValueServiceConfig KVS_CONFIG = mock(KeyValueServiceConfig.class);
    private static final ServerListConfig DEFAULT_SERVER_LIST = ImmutableServerListConfig.builder()
            .addServers("http://" + WireMockConfiguration.DEFAULT_BIND_ADDRESS + ":" + PORT)
            .build();
    private static final TimeLockClientConfig TIMELOCK_CONFIG = ImmutableTimeLockClientConfig.builder()
            .client("testClient")
            .serversList(DEFAULT_SERVER_LIST)
            .build();
    private static final AtlasDbConfig CANONICAL_TIMELOCK_CONFIG = ImmutableAtlasDbConfig.builder()
            .keyValueService(KVS_CONFIG)
            .timelock(TIMELOCK_CONFIG)
            .build();

    private final TimestampStoreInvalidator invalidator = mock(TimestampStoreInvalidator.class);

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(PORT);

    @Before
    public void setUp() {
        when(invalidator.backupAndInvalidate()).thenReturn(BACKUP_TIMESTAMP);

        wireMockRule.stubFor(PING_MAPPING.willReturn(aResponse().withStatus(200).withBody("\"pong\"")));
    }

    @Test
    public void throwsIfCreatingMigratorFromEmbeddedConfig() {
        AtlasDbConfig embeddedConfig = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .build();
        assertThatThrownBy(() -> TimelockMigrator.create(embeddedConfig, invalidator))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void throwsIfCreatingMigratorFromLeaderConfig() {
        AtlasDbConfig leaderConfig = ImmutableAtlasDbConfig.builder()
                .keyValueService(KVS_CONFIG)
                .leader(mock(LeaderConfig.class))
                .build();
        assertThatThrownBy(() -> TimelockMigrator.create(leaderConfig, invalidator))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void propagatesBackupTimestampToFastForwardOnRemoteService() {
        wireMockRule.stubFor(TEST_MAPPING.willReturn(aResponse().withStatus(204)));

        TimelockMigrator migrator = TimelockMigrator.create(CANONICAL_TIMELOCK_CONFIG, invalidator);
        migrator.migrate();

        wireMockRule.verify(getRequestedFor(urlEqualTo(PING_ENDPOINT)));
        verify(invalidator, times(1)).backupAndInvalidate();
        wireMockRule.verify(postRequestedFor(urlEqualTo(TEST_ENDPOINT)));
    }

    @Test
    public void invalidationDoesNotProceedIfTimelockPingUnsuccessful() {
        wireMockRule.stubFor(PING_MAPPING.willReturn(aResponse().withFault(Fault.MALFORMED_RESPONSE_CHUNK)));

        TimelockMigrator migrator = TimelockMigrator.create(CANONICAL_TIMELOCK_CONFIG, invalidator);
        assertThatThrownBy(migrator::migrate).isInstanceOf(IllegalStateException.class);
        verify(invalidator, never()).backupAndInvalidate();
    }

    @Test
    public void migrationDoesNotProceedIfInvalidationFails() {
        when(invalidator.backupAndInvalidate()).thenThrow(new IllegalStateException());

        TimelockMigrator migrator = TimelockMigrator.create(CANONICAL_TIMELOCK_CONFIG, invalidator);
        assertThatThrownBy(migrator::migrate).isInstanceOf(IllegalStateException.class);
        wireMockRule.verify(0, postRequestedFor(urlEqualTo(TEST_ENDPOINT)));
    }
}

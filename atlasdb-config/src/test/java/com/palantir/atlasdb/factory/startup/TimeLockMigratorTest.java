/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ImmutableTimeLockClientConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.config.TimeLockClientConfig;
import com.palantir.common.exception.AtlasDbDependencyException;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampStoreInvalidator;

public class TimeLockMigratorTest {
    private static final long BACKUP_TIMESTAMP = 42;
    private static final String TEST_ENDPOINT = "/testClient/timestamp-management/fast-forward?currentTimestamp="
            + BACKUP_TIMESTAMP;
    private static final String PING_ENDPOINT = "/testClient/timestamp-management/ping";
    private static final MappingBuilder TEST_MAPPING = post(urlEqualTo(TEST_ENDPOINT));
    private static final MappingBuilder PING_MAPPING = get(urlEqualTo(PING_ENDPOINT));
    private static final String SCENARIO = "scenario";

    private static final String USER_AGENT = "user-agent (123456789)";

    private TimeLockClientConfig timelockConfig;

    private final TimestampStoreInvalidator invalidator = mock(TimestampStoreInvalidator.class);

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(WireMockConfiguration.wireMockConfig().dynamicPort());

    @Before
    public void setUp() {
        when(invalidator.backupAndInvalidate()).thenReturn(BACKUP_TIMESTAMP);
        wireMockRule.stubFor(PING_MAPPING.willReturn(aResponse()
                .withStatus(200)
                .withBody(TimestampManagementService.PING_RESPONSE)
                .withHeader("Content-Type", "text/plain"))
                .inScenario(SCENARIO)
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo(Scenario.STARTED));

        String serverUri = String.format("http://%s:%s",
                WireMockConfiguration.DEFAULT_BIND_ADDRESS,
                wireMockRule.port());
        ServerListConfig defaultServerListConfig = ImmutableServerListConfig.builder().addServers(serverUri).build();
        timelockConfig = ImmutableTimeLockClientConfig.builder()
                .client("testClient")
                .serversList(defaultServerListConfig)
                .build();
    }

    @Test
    public void propagatesBackupTimestampToFastForwardOnRemoteService() {
        wireMockRule.stubFor(TEST_MAPPING.willReturn(aResponse().withStatus(204)));

        TimeLockMigrator migrator =
                TimeLockMigrator.create(timelockConfig.toNamespacedServerList(), () -> null, invalidator, USER_AGENT);
        migrator.migrate();

        wireMockRule.verify(getRequestedFor(urlEqualTo(PING_ENDPOINT)));
        verify(invalidator, times(1)).backupAndInvalidate();
        wireMockRule.verify(postRequestedFor(urlEqualTo(TEST_ENDPOINT)));
    }

    @Test
    public void invalidationDoesNotProceedIfTimelockPingUnsuccessful() {
        wireMockRule.stubFor(PING_MAPPING.willReturn(aResponse().withStatus(500)));

        TimeLockMigrator migrator =
                TimeLockMigrator.create(timelockConfig.toNamespacedServerList(), () -> null, invalidator, USER_AGENT);
        assertThatThrownBy(migrator::migrate).isInstanceOf(AtlasDbDependencyException.class);
        verify(invalidator, never()).backupAndInvalidate();
    }

    @Test
    public void migrationDoesNotProceedIfInvalidationFails() {
        when(invalidator.backupAndInvalidate()).thenThrow(new IllegalStateException());

        TimeLockMigrator migrator =
                TimeLockMigrator.create(timelockConfig.toNamespacedServerList(), () -> null, invalidator, USER_AGENT);
        assertThatThrownBy(migrator::migrate).isInstanceOf(IllegalStateException.class);
        wireMockRule.verify(0, postRequestedFor(urlEqualTo(TEST_ENDPOINT)));
    }

    @Test
    public void asyncMigrationProceedsIfTimeLockInitiallyUnavailable() throws InterruptedException {
        String nowSucceeding = "nowSucceeding";

        wireMockRule.stubFor(PING_MAPPING.inScenario(SCENARIO)
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withStatus(500))
                .willSetStateTo(nowSucceeding));

        wireMockRule.stubFor(PING_MAPPING.inScenario(SCENARIO)
                .whenScenarioStateIs(nowSucceeding)
                .willReturn(aResponse().withStatus(204)));

        wireMockRule.stubFor(TEST_MAPPING.willReturn(aResponse().withStatus(204)));

        TimeLockMigrator migrator =
                TimeLockMigrator.create(() -> timelockConfig.toNamespacedServerList(), () -> null, invalidator, USER_AGENT, true);
        migrator.migrate();

        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(migrator::isInitialized);

        wireMockRule.verify(getRequestedFor(urlEqualTo(PING_ENDPOINT)));
        verify(invalidator, times(1)).backupAndInvalidate();
        wireMockRule.verify(postRequestedFor(urlEqualTo(TEST_ENDPOINT)));
    }

    @Test
    public void asyncMigrationProceedsIfInvalidatorInitiallyUnavailable() throws InterruptedException {
        when(invalidator.backupAndInvalidate())
                .thenAnswer(new Answer<Long>() {
                    private AtomicBoolean shouldFail = new AtomicBoolean(true);
                    @Override
                    public Long answer(InvocationOnMock invocation) throws Throwable {
                        if (shouldFail.getAndSet(false)) {
                            throw new IllegalStateException("not ready yet");
                        }
                        return BACKUP_TIMESTAMP;
                    }
                });

        wireMockRule.stubFor(TEST_MAPPING.willReturn(aResponse().withStatus(204)));
        TimeLockMigrator migrator =
                TimeLockMigrator.create(() -> timelockConfig.toNamespacedServerList(), () -> null, invalidator, USER_AGENT, true);
        migrator.migrate();

        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(migrator::isInitialized);

        wireMockRule.verify(getRequestedFor(urlEqualTo(PING_ENDPOINT)));
        verify(invalidator, times(2)).backupAndInvalidate();
        wireMockRule.verify(postRequestedFor(urlEqualTo(TEST_ENDPOINT)));
    }
}

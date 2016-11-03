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
package com.palantir.atlasdb.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSortedMap;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.Duration;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.containers.PostgresContainer;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.server.config.AtlasDbServerConfiguration;
import com.palantir.atlasdb.server.config.ClientConfig;
import com.palantir.lock.AtlasTimestampLockDescriptor;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.timestamp.TimestampService;

import feign.FeignException;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;

@RunWith(Parameterized.class)
public class AtlasDbServerTest {
    private static final Containers CONTAINERS =
            new Containers(AtlasDbServerTest.class)
                    .with(new CassandraContainer())
                    .with(new PostgresContainer());

    private static final DropwizardAppRule<AtlasDbServerConfiguration> APP = new DropwizardAppRule<>(
            AtlasDbServer.class,
            ResourceHelpers.resourceFilePath("testServer.yml"));

    @ClassRule
    public static final RuleChain RULES = RuleChain.outerRule(CONTAINERS)
            .around(APP);

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> serverClients() throws Exception {
        return java.util.Arrays.asList(new Object[][] {
                { "cassandra" },
                { "postgres" }
        });
    }

    private final String client;

    public AtlasDbServerTest(String client) {
        this.client = client;
    }

    @BeforeClass
    public static void waitForStartup() {
        APP.getConfiguration().clients().forEach(AtlasDbServerTest::waitForClientStartup);
    }

    @Test
    public void shouldBeAbleToGetTimestampsFromTheServer() {
        TimestampService timestampService = constructTimestampServiceClient(client);

        long timestamp1 = timestampService.getFreshTimestamp();
        long timestamp2 = timestampService.getFreshTimestamp();

        assertThat(timestamp1).isLessThan(timestamp2);
    }

    @Test
    public void shouldBeAbleToAcquireLocksFromTheServer() throws InterruptedException {
        LockDescriptor descriptor = AtlasTimestampLockDescriptor.of(1);
        SortedMap<LockDescriptor, LockMode> locksToAcquire =
                ImmutableSortedMap.of(descriptor, LockMode.READ);
        LockRequest lockRequest = LockRequest.builder(locksToAcquire)
                .blockForAtMost(SimpleTimeDuration.of(1, TimeUnit.MINUTES))
                .build();

        RemoteLockService remoteLockService = constructLockServiceClient(client);
        HeldLocksToken token = remoteLockService.lockAndGetHeldLocks(client, lockRequest);
        try {
            assertThat(token.getLockDescriptors()).containsOnly(descriptor);
        } finally {
            remoteLockService.unlock(token.getLockRefreshToken());
        }
    }

    private static TimestampService constructTimestampServiceClient(String client) {
        return AtlasDbHttpClients.createProxy(
                Optional.absent(),
                getUriForClient(client),
                TimestampService.class);
    }

    private static RemoteLockService constructLockServiceClient(String client) {
        return AtlasDbHttpClients.createProxy(
                Optional.absent(),
                getUriForClient(client),
                RemoteLockService.class);
    }

    private static String getUriForClient(String client) {
        return "http://localhost:" + APP.getLocalPort() + "/" + client;
    }

    private static void waitForClientStartup(ClientConfig client) {
        Awaitility.await()
                .atMost(Duration.FIVE_MINUTES)
                .pollInterval(Duration.FIVE_SECONDS)
                .until(serverIsHealthy());
    }

    private static Callable<Boolean> serverIsHealthy() {
        HealthcheckResource healthcheckResource = AtlasDbHttpClients.createProxy(
                Optional.absent(),
                "http://localhost:" + APP.getAdminPort(),
                HealthcheckResource.class);
        return () -> {
            try {
                healthcheckResource.get();
                return true;
            } catch (FeignException e) {
                return false;
            }
        };
    }
}

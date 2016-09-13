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
import java.util.function.Supplier;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.base.Optional;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.Duration;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.server.config.AtlasDbServerConfiguration;
import com.palantir.atlasdb.server.config.ClientConfig;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.logging.FileLogCollector;
import com.palantir.timestamp.TimestampService;

import feign.FeignException;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;

@RunWith(Parameterized.class)
public class AtlasDbServerTest {
    private static final DockerComposeRule DOCKER_COMPOSE_RULE = DockerComposeRule.builder()
            .file("docker/services.yml")
            .logCollector(FileLogCollector.fromPath("logs"))
            .build();

    private static final DropwizardAppRule<AtlasDbServerConfiguration> APP = new DropwizardAppRule<>(
            AtlasDbServer.class,
            ResourceHelpers.resourceFilePath("testServer.yml"));

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> serverClients() throws Exception {
        return java.util.Arrays.asList(new Object[][] {
                { "cassandra" },
                { "postgres" },
                { "memory" },
                { "rocksdb" }
        });
    }

    @ClassRule
    public static final RuleChain RULES = RuleChain.outerRule(DOCKER_COMPOSE_RULE)
            .around(new DockerProxyRule(DOCKER_COMPOSE_RULE))
            .around(APP);

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

    private static void waitForClientStartup(ClientConfig client) {
        TimestampService timestamp = constructTimestampServiceClient(client.client());

        Awaitility.await()
                .atMost(Duration.FIVE_MINUTES)
                .pollInterval(Duration.FIVE_SECONDS)
                .until(() -> isEndpointReady(timestamp::getFreshTimestamp));
    }

    private static TimestampService constructTimestampServiceClient(String client) {
        return AtlasDbHttpClients.createProxy(
                Optional.absent(),
                "http://localhost:" + APP.getLocalPort() + "/" + client,
                TimestampService.class);
    }

    private static boolean isEndpointReady(Supplier<?> fn) {
        try {
            fn.get();
            return true;
        } catch (FeignException e) {
            return false;
        }
    }
}

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
package com.palantir.atlasdb.dropwizard.commands;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Throwables;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbConfigs;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.dropwizard.AtlasDbConfigurationProvider;
import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;

import io.dropwizard.Configuration;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;

public class AtlasDbConsoleCommandTest {
    private static final String LOCAL_SERVER_NAME = "Local Server";
    private static final AtlasDbConfig MINIMAL_LEADER_CONFIG = ImmutableAtlasDbConfig.builder()
            .leader(ImmutableLeaderConfig.builder()
                    .quorumSize(1)
                    .addLeaders(LOCAL_SERVER_NAME)
                    .localServer(LOCAL_SERVER_NAME)
                    .build())
            .keyValueService(new InMemoryAtlasDbConfig())
            .build();

    private static final Map<String, Object> ONLINE_PARAMS = Collections
            .singletonMap(AtlasDbCommandUtils.OFFLINE_COMMAND_ARG_NAME, null);
    private static final Map<String, Object> OFFLINE_PARAMS = Collections
            .singletonMap(AtlasDbCommandUtils.OFFLINE_COMMAND_ARG_NAME, AtlasDbCommandUtils.ZERO_ARITY_ARG_CONSTANT);

    @Test
    public void lockAndTimestampFieldsShouldBeSetWhenRunningOnline() throws JsonProcessingException {
        AtlasDbConfig cliConfig = getConfigFromConsoleCommand(ONLINE_PARAMS, MINIMAL_LEADER_CONFIG);

        assertThat(cliConfig.lock().isPresent()).describedAs("Lock block must be present").isTrue();
        assertThat(cliConfig.timestamp().isPresent()).describedAs("Timestamp block must be present").isTrue();
    }

    @Test
    public void lockAndTimestampFieldsShouldBeEmptyWhenRunningOffline() throws JsonProcessingException {
        AtlasDbConfig cliConfig = getConfigFromConsoleCommand(OFFLINE_PARAMS, MINIMAL_LEADER_CONFIG);

        assertThat(cliConfig.lock().isPresent()).describedAs("Lock block must be absent").isFalse();
        assertThat(cliConfig.timestamp().isPresent()).describedAs("Timestamp block must be absent").isFalse();
    }

    private AtlasDbConfig getConfigFromConsoleCommand(Map<String, Object> params, AtlasDbConfig config)
            throws JsonProcessingException {
        AtomicReference<AtlasDbConfig> cliConfig = new AtomicReference<>();

        new AtlasDbConsoleCommand<AtlasDbDropwizardConfig>(AtlasDbDropwizardConfig.class) {
            @Override
            protected void runAtlasDbConsole(List<String> allArgs) {
                try {
                    cliConfig.set(AtlasDbConfigs.loadFromString(allArgs.get(2), null));
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        }.run(mock(Bootstrap.class), new Namespace(params), new AtlasDbDropwizardConfig(config));

        return cliConfig.get();
    }

    private class AtlasDbDropwizardConfig extends Configuration implements AtlasDbConfigurationProvider {
        private final AtlasDbConfig atlasDbConfig;

        AtlasDbDropwizardConfig(AtlasDbConfig atlasDbConfig) {
            this.atlasDbConfig = atlasDbConfig;
        }

        @Override
        public AtlasDbConfig getAtlasDbConfig() {
            return atlasDbConfig;
        }

        @Override
        public Optional<AtlasDbRuntimeConfig> getAtlasDbRuntimeConfig() {
            return Optional.empty();
        }
    }
}

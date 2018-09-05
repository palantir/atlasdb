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

package com.palantir.atlasdb.timelock.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk7.Jdk7Module;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.timelock.config.TsBoundPersisterConfiguration;

import io.dropwizard.jackson.DiscoverableSubtypeResolver;

public abstract class AbstractTimelockServerConfigDeserializationTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory()
            .disable(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID)
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));

    static {
        OBJECT_MAPPER.setSubtypeResolver(new DiscoverableSubtypeResolver());
        OBJECT_MAPPER.registerModule(new GuavaModule());
        OBJECT_MAPPER.registerModule(new Jdk7Module());
        OBJECT_MAPPER.registerModule(new Jdk8Module());
    }

    protected AbstractTimelockServerConfigDeserializationTest() {}

    public abstract String getConfigFileName();

    public abstract void assertTimestampBoundPersisterConfigurationCorrect(TsBoundPersisterConfiguration configuration);

    @Test
    public void canDeserializeTimeLockServerConfiguration() throws IOException {
        File testConfigFile = new File(
                AbstractTimelockServerConfigDeserializationTest.class.getResource(getConfigFileName()).getPath());
        TimeLockServerConfiguration configuration =
                OBJECT_MAPPER.readValue(testConfigFile, TimeLockServerConfiguration.class);

        assertThat(configuration.cluster().localServer()).isEqualTo("localhost:8080");
        assertThat(configuration.cluster().servers()).containsExactlyInAnyOrder(
                "localhost:8080", "localhost:8081", "localhost:8082");

        assertThat(configuration.clients()).containsExactlyInAnyOrder("test", "test2", "test3", "acceptor", "learner");

        assertAlgorithmConfigurationCorrect(configuration.algorithm());

        assertTimestampBoundPersisterConfigurationCorrect(configuration.getTsBoundPersisterConfiguration());

        assertThat(configuration.useClientRequestLimit()).isTrue();

        assertThat(configuration.timeLimiterConfiguration().enableTimeLimiting()).isTrue();
        assertThat(configuration.timeLimiterConfiguration().blockingTimeoutErrorMargin()).isEqualTo(0.03);

        assertThat(configuration.asyncLockConfiguration().useAsyncLockService()).isTrue();
        assertThat(configuration.asyncLockConfiguration().disableLegacySafetyChecksWarningPotentialDataCorruption())
                .isFalse();
    }

    public void assertAlgorithmConfigurationCorrect(TimeLockAlgorithmConfiguration configuration) {
        assertThat(configuration).isInstanceOf(PaxosConfiguration.class);

        PaxosConfiguration paxosConfiguration = (PaxosConfiguration) configuration;

        assertSslConfigurationCorrect(paxosConfiguration.sslConfiguration().get());
        assertThat(paxosConfiguration.paxosDataDir()).isEqualTo(Paths.get("var", "data", "paxos").toFile());
    }

    private void assertSslConfigurationCorrect(SslConfiguration sslConfiguration) {
        assertThat(sslConfiguration.trustStorePath()).isEqualTo(Paths.get("var", "security", "trustStore.jks"));
        assertThat(sslConfiguration.keyStorePath().isPresent()).isFalse();
        assertThat(sslConfiguration.keyStorePassword().isPresent()).isFalse();
    }
}

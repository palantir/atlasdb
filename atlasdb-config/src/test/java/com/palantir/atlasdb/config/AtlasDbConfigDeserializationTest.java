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

package com.palantir.atlasdb.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import org.junit.Test;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.remoting2.config.ssl.SslConfiguration;

public class AtlasDbConfigDeserializationTest {
    private static final File TEST_CONFIG_FILE = new File(
            AtlasDbConfigDeserializationTest.class.getResource("/test-config.yml").getPath());
    private static final File MINIMAL_TEST_CONFIG_FILE = new File(
            AtlasDbConfigDeserializationTest.class.getResource("/test-config-minimal.yml").getPath());

    @Test
    public void canDeserializeAtlasDbConfig() throws IOException {
        AtlasDbConfig config = AtlasDbConfigs.load(TEST_CONFIG_FILE);
        assertThat(config.namespace().get()).isEqualTo("test");
        assertThat(config.keyValueService()).isEqualTo(new InMemoryAtlasDbConfig());

        assertThat(config.timelock().isPresent()).isTrue();
        assertTimeLockConfigDeserializedCorrectly(config.timelock().get());

        assertThat(config.leader().isPresent()).isFalse();
        assertThat(config.enableSweep()).isTrue();
    }

    @Test
    public void canDeserializeMinimalAtlasDbConfig() throws IOException {
        AtlasDbConfig config = AtlasDbConfigs.load(MINIMAL_TEST_CONFIG_FILE);
        assertThat(config.namespace().isPresent()).isFalse();
        assertThat(config.keyValueService()).isEqualTo(new InMemoryAtlasDbConfig());

        assertThat(config.timelock().isPresent()).isFalse();
        assertThat(config.leader().isPresent()).isFalse();

        assertThat(config.enableSweep()).isEqualTo(AtlasDbConstants.DEFAULT_ENABLE_SWEEP);
    }

    private void assertTimeLockConfigDeserializedCorrectly(TimeLockClientConfig timeLockClientConfig) {
        assertThat(timeLockClientConfig.getClientOrThrow()).isEqualTo("test");
        assertThat(timeLockClientConfig.serversList().servers()).containsExactlyInAnyOrder(
                "timelock1:8080", "timelock2:8080", "timelock3:8080");
        assertThat(timeLockClientConfig.serversList().sslConfiguration().isPresent()).isTrue();

        SslConfiguration sslConfiguration = timeLockClientConfig.serversList().sslConfiguration().get();
        assertSslConfigDeserializedCorrectly(sslConfiguration);
    }

    private void assertSslConfigDeserializedCorrectly(SslConfiguration sslConfiguration) {
        assertThat(sslConfiguration.keyStorePassword()).hasValue("1234567890");
        assertThat(sslConfiguration.keyStorePath()).hasValue(Paths.get("var", "security", "keyStore.jks"));
        assertThat(sslConfiguration.trustStorePath()).isEqualTo(Paths.get("var", "security", "trustStore.jks"));
    }
}

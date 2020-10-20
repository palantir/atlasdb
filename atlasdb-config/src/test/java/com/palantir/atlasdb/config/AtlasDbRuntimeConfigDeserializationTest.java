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
package com.palantir.atlasdb.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import org.junit.Test;

public class AtlasDbRuntimeConfigDeserializationTest {
    private static final File TEST_RUNTIME_CONFIG_FILE = new File(AtlasDbRuntimeConfigDeserializationTest.class
            .getResource("/runtime-config-block.yml")
            .getPath());
    private static final File TEST_RUNTIME_CONFIG_NO_SERVERS_FILE =
            new File(AtlasDbRuntimeConfigDeserializationTest.class
                    .getResource("/runtime-config-block-no-servers.yml")
                    .getPath());

    @Test
    public void canDeserializeRuntimeConfig() throws IOException {
        AtlasDbRuntimeConfig runtimeConfig =
                AtlasDbConfigs.OBJECT_MAPPER.readValue(TEST_RUNTIME_CONFIG_FILE, AtlasDbRuntimeConfig.class);
        assertThat(runtimeConfig.timestampClient().enableTimestampBatching()).isTrue();

        assertThat(runtimeConfig.timelockRuntime()).isPresent();
        assertThat(runtimeConfig.timelockRuntime().get().serversList().servers())
                .containsExactlyInAnyOrder("https://foo1:12345", "https://foo2:8421", "https://foo3:9421");
        assertThat(runtimeConfig.timelockRuntime().get().serversList().sslConfiguration())
                .satisfies(sslConfiguration -> sslConfiguration.ifPresent(this::assertSslConfigDeserializedCorrectly));
        assertThat(runtimeConfig.streamStorePersistence()).satisfies(persistenceConfig -> {
            assertThat(persistenceConfig.numBlocksToWriteBeforePause()).isEqualTo(7);
            assertThat(persistenceConfig.writePauseDurationMillis()).isEqualTo(77);
        });
        assertThat(runtimeConfig.sweep().sweepPriorityOverrides()).satisfies(overrides -> {
            assertThat(overrides.priorityTables()).containsExactlyInAnyOrder("atlas.mission_critical_table");
            assertThat(overrides.blacklistTables())
                    .containsExactlyInAnyOrder("atlas.bad_table", "atlas2.immutable_log");
        });

        assertThat(runtimeConfig.internalSchema().targetTransactionsSchemaVersion())
                .contains(1);
    }

    private void assertSslConfigDeserializedCorrectly(SslConfiguration sslConfiguration) {
        assertThat(sslConfiguration.keyStorePassword()).hasValue("0987654321");
        assertThat(sslConfiguration.keyStorePath()).hasValue(Paths.get("var", "security", "keyStore.jks"));
        assertThat(sslConfiguration.trustStorePath()).isEqualTo(Paths.get("var", "security", "trustStore.jks"));
    }

    @Test
    public void canDeserializeRuntimeConfigWithZeroServers() throws IOException {
        AtlasDbRuntimeConfig runtimeConfig =
                AtlasDbConfigs.OBJECT_MAPPER.readValue(TEST_RUNTIME_CONFIG_NO_SERVERS_FILE, AtlasDbRuntimeConfig.class);
        assertThat(runtimeConfig.timestampClient().enableTimestampBatching()).isTrue();

        assertThat(runtimeConfig.timelockRuntime()).isPresent();
        assertThat(runtimeConfig.timelockRuntime().get().serversList().servers())
                .isEmpty();
        assertThat(runtimeConfig.timelockRuntime().get().serversList().sslConfiguration())
                .isEmpty();
    }
}

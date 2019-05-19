/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.auth.config;


import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.palantir.atlasdb.timelock.auth.api.BCryptedSecret;
import com.palantir.atlasdb.timelock.auth.api.Credentials;
import com.palantir.atlasdb.timelock.auth.api.ImmutableCredentials;

public class TimelockAuthConfigurationTest {
    private static final String TIMELOCK_AUTH_CONFIG = "timelock-auth-config";

    private static final Credentials CLIENT_CREDENTIALS = ImmutableCredentials.builder()
            .id("client-1")
            .password(BCryptedSecret.of("hashed-client-secret"))
            .build();
    private static final Credentials ADMIN_CREDENTIALS = ImmutableCredentials.builder()
            .id("admin-user-1")
            .password(BCryptedSecret.of("hashed-admin-secret"))
            .build();

    private static final PrivilegesConfiguration CLIENT_PRIVILEGES_CONFIG = ImmutableClientPrivilegesConfiguration.builder()
            .id("client-1")
            .addNamespaces("namespace-1", "namespace-2")
            .build();
    private static final PrivilegesConfiguration ADMIN_PRIVILEGES_CONFIG = ImmutableAdminPrivilegesConfiguration.builder()
            .id("admin-user-1")
            .build();

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .registerModule(new Jdk8Module())
            .registerModule(new GuavaModule());

    @Test
    public void canDeserializeTimelockAuthConfig() throws IOException {
        assertThat(deserialize(getConfigFile(TIMELOCK_AUTH_CONFIG)))
                .isInstanceOf(TimelockAuthConfiguration.class);
    }

    @Test
    public void credentialsAreDeserialized() throws IOException {
        TimelockAuthConfiguration timelockAuthConfiguration = deserialize(getConfigFile(TIMELOCK_AUTH_CONFIG));

        List<Credentials> credentials = timelockAuthConfiguration.clientAuthCredentials();
        assertThat(credentials).containsExactly(CLIENT_CREDENTIALS, ADMIN_CREDENTIALS);
    }

    @Test
    public void privilegesAreDeserialized() throws IOException {
        TimelockAuthConfiguration timelockAuthConfiguration = deserialize(getConfigFile(TIMELOCK_AUTH_CONFIG));

        List<PrivilegesConfiguration> privilegesConfigurations = timelockAuthConfiguration.privileges();
        assertThat(privilegesConfigurations).containsExactly(
                ADMIN_PRIVILEGES_CONFIG,
                CLIENT_PRIVILEGES_CONFIG);
    }

    @Test
    public void useAuthIsConfigured() throws IOException {
        TimelockAuthConfiguration timelockAuthConfiguration = deserialize(getConfigFile(TIMELOCK_AUTH_CONFIG));

        assertThat(timelockAuthConfiguration.useAuth()).isTrue();
    }

    private static TimelockAuthConfiguration deserialize(File configFile) throws IOException {
        return mapper.readValue(configFile, TimelockAuthConfiguration.class);
    }

    private static File getConfigFile(String configFile) {
        return new File(PrivilegesConfigurationTest.class.getResource(
                String.format("/%s.yml", configFile)).getPath());
    }
}
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.palantir.atlasdb.timelock.auth.api.BCryptedSecret;
import com.palantir.atlasdb.timelock.auth.api.ClientId;
import com.palantir.atlasdb.timelock.auth.api.Credentials;
import com.palantir.atlasdb.timelock.auth.api.ImmutableCredentials;
import com.palantir.lock.TimelockNamespace;
import java.io.File;
import java.io.IOException;
import org.junit.Test;

public class TimelockAuthConfigurationTest {
    private static final String TIMELOCK_AUTH_CONFIG = "timelock-auth-config";

    private static final boolean USE_AUTH = true;

    private static final Credentials CLIENT_CREDENTIALS = ImmutableCredentials.builder()
            .id(ClientId.of("client-1"))
            .password(BCryptedSecret.of("hashed-client-secret"))
            .build();
    private static final Credentials ADMIN_CREDENTIALS = ImmutableCredentials.builder()
            .id(ClientId.of("admin-user-1"))
            .password(BCryptedSecret.of("hashed-admin-secret"))
            .build();

    private static final PrivilegesConfiguration CLIENT_PRIVILEGES_CONFIG = ImmutableClientPrivilegesConfiguration.builder()
            .clientId(ClientId.of("client-1"))
            .addNamespaces(TimelockNamespace.of("namespace-1"), TimelockNamespace.of("namespace-2"))
            .build();
    private static final PrivilegesConfiguration ADMIN_PRIVILEGES_CONFIG = ImmutableAdminPrivilegesConfiguration.builder()
            .clientId(ClientId.of("admin-user-1"))
            .build();

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .registerModule(new Jdk8Module())
            .registerModule(new GuavaModule());

    @Test
    public void deserializedAsExpected() throws IOException {
        TimelockAuthConfiguration expectedConfig = ImmutableTimelockAuthConfiguration.builder()
                .useAuth(USE_AUTH)
                .addPrivileges(ADMIN_PRIVILEGES_CONFIG, CLIENT_PRIVILEGES_CONFIG)
                .addCredentials(ADMIN_CREDENTIALS, CLIENT_CREDENTIALS)
                .build();

        TimelockAuthConfiguration deserializedConfig = deserialize(getConfigFile(TIMELOCK_AUTH_CONFIG));

        assertThat(deserializedConfig).isEqualTo(expectedConfig);
    }

    private static TimelockAuthConfiguration deserialize(File configFile) throws IOException {
        return mapper.readValue(configFile, TimelockAuthConfiguration.class);
    }

    private static File getConfigFile(String configFile) {
        return new File(TimelockAuthConfiguration.class.getResource(
                String.format("/%s.yml", configFile)).getPath());
    }
}
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
import com.palantir.atlasdb.timelock.auth.api.ClientId;
import com.palantir.lock.TimelockNamespace;
import java.io.File;
import java.io.IOException;
import org.junit.Test;

public class PrivilegesConfigurationTest {
    private static final String ADMIN_PRIVILEGES_CONFIG = "admin-privileges-config";
    private static final ClientId ADMIN_ID = ClientId.of("admin-user-1");

    private static final String CLIENT_PRIVILEGES_CONFIG = "client-privileges-config";
    private static final ClientId CLIENT_ID = ClientId.of("client-1");

    private static final TimelockNamespace CLIENT_NAMESPACE_1 = TimelockNamespace.of("namespace-1");
    private static final TimelockNamespace CLIENT_NAMESPACE_2 = TimelockNamespace.of("namespace-2");


    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .registerModule(new Jdk8Module())
            .registerModule(new GuavaModule());

    @Test
    public void canDeserializeAdminConfig() throws IOException {
        PrivilegesConfiguration deserializedConfiguration = deserialize(getConfigFile(ADMIN_PRIVILEGES_CONFIG));

        AdminPrivilegesConfiguration expectedConfiguration = ImmutableAdminPrivilegesConfiguration.builder()
                .clientId(ADMIN_ID)
                .build();

        assertThat(deserializedConfiguration).isEqualTo(expectedConfiguration);
    }

    @Test
    public void canDeserializeClientConfig() throws IOException {
        PrivilegesConfiguration deserializedConfiguration = deserialize(getConfigFile(CLIENT_PRIVILEGES_CONFIG));

        ClientPrivilegesConfiguration expectedConfiguration = ImmutableClientPrivilegesConfiguration.builder()
                .clientId(CLIENT_ID)
                .addNamespaces(CLIENT_NAMESPACE_1, CLIENT_NAMESPACE_2)
                .build();

        assertThat(deserializedConfiguration).isEqualTo(expectedConfiguration);
    }

    private static PrivilegesConfiguration deserialize(File configFile) throws IOException {
        return mapper.readValue(configFile, PrivilegesConfiguration.class);
    }

    private static File getConfigFile(String configFile) {
        return new File(PrivilegesConfigurationTest.class.getResource(
                String.format("/%s.yml", configFile)).getPath());
    }
}

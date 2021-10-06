/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.config.crypto.KeyFileUtils;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class AtlasDbConfigsTest {
    private static String previousKeyPathProperty;

    @BeforeClass
    public static void setUpClass() throws URISyntaxException {
        previousKeyPathProperty = System.getProperty(KeyFileUtils.KEY_PATH_PROPERTY);
        System.setProperty(
                KeyFileUtils.KEY_PATH_PROPERTY,
                Paths.get(AtlasDbConfigsTest.class.getResource("/test.key").toURI())
                        .toString());
    }

    @AfterClass
    public static void tearDownClass() {
        if (previousKeyPathProperty != null) {
            System.setProperty(KeyFileUtils.KEY_PATH_PROPERTY, previousKeyPathProperty);
        }
    }

    @Test
    public void canDecryptValues() throws IOException {
        AtlasDbConfig config = AtlasDbConfigs.load(
                new File(AtlasDbConfigsTest.class
                        .getResource("/encrypted-config.yml")
                        .getPath()),
                AtlasDbConfig.class);
        KeyValueServiceConfig kvsConfig = config.keyValueService();
        assertThat(kvsConfig).isInstanceOf(InMemoryAtlasDbConfig.class);
    }

    @Test
    public void testDiscoverKvsConfigSubtypes() {
        DiscoverableSubtypeResolver subtypeResolver =
                new DiscoverableSubtypeResolver(AtlasDbConfigs.DISCOVERED_SUBTYPE_MARKER);
        assertThat(subtypeResolver.getDiscoveredSubtypes()).contains(InMemoryAtlasDbConfig.class);
        List<Class<?>> allDiscoveredSubtypes = subtypeResolver.getDiscoveredSubtypes();
        Set<Class<?>> discoveredKvsConfigs = allDiscoveredSubtypes.stream()
                .filter(KeyValueServiceConfig.class::isAssignableFrom)
                .collect(Collectors.toSet());
        assertThat(discoveredKvsConfigs)
                .containsExactlyInAnyOrder(InMemoryAtlasDbConfig.class, InMemoryAsyncAtlasDbConfig.class);
    }
}

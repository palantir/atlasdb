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
package com.palantir.atlasdb.services;

import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbConfigs;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import org.junit.Test;

public class AtlasDbServicesTest {

    @Test
    public void daggerCanInstantiateAtlas() throws URISyntaxException, IOException {
        File config = new File(getResourcePath("simple_atlas_config.yml"));
        ServicesConfigModule servicesConfigModule = ServicesConfigModule.create(
                AtlasDbConfigs.load(config, AtlasDbConfig.class), AtlasDbRuntimeConfig.defaultRuntimeConfig());
        DaggerAtlasDbServices.builder()
                .servicesConfigModule(servicesConfigModule)
                .build();
    }

    private static String getResourcePath(String fileName) throws URISyntaxException {
        return Paths.get(AtlasDbServicesTest.class
                        .getClassLoader()
                        .getResource(fileName)
                        .toURI())
                .toString();
    }
}

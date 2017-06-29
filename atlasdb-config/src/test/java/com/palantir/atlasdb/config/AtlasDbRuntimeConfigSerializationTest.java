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

import org.junit.Test;

public class AtlasDbRuntimeConfigSerializationTest {
    private static final File TEST_RUNTIME_CONFIG_FILE = new File(
            AtlasDbConfigDeserializationTest.class.getResource("/runtime-config-block.yml").getPath());

    @Test
    public void canDeserializeAtlasDbConfig() throws IOException {
        AtlasDbRuntimeConfig runtimeConfig =
                AtlasDbConfigs.OBJECT_MAPPER.readValue(TEST_RUNTIME_CONFIG_FILE, AtlasDbRuntimeConfig.class);
        assertThat(runtimeConfig.timestampClient().enableTimestampBatching()).isTrue();
    }
}

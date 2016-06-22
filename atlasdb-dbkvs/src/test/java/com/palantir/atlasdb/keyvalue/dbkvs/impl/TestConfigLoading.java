/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.palantir.atlasdb.config.AtlasDbConfigs;

public class TestConfigLoading {
    @Test
    public void testLoadingConfig() throws IOException {
        AtlasDbConfigs.load(new File(getClass().getClassLoader().getResource("postgresTestConfig.yml").getFile()));
    }
}

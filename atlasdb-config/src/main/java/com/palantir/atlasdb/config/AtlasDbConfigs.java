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
package com.palantir.atlasdb.config;

import java.io.File;
import java.io.IOException;

import javax.annotation.Nullable;

public final class AtlasDbConfigs {
    public static final String ATLASDB_CONFIG_ROOT = "/atlasdb";

    private AtlasDbConfigs() {
        // uninstantiable
    }

    public static AtlasDbConfig load(File configFile) throws IOException {
        return load(configFile, ATLASDB_CONFIG_ROOT);
    }

    public static AtlasDbConfig load(File configFile, @Nullable String configRoot) throws IOException {
        ConfigFinder finder = new ConfigFinder(configRoot);
        return finder.getConfig(configFile);
    }

    public static AtlasDbConfig loadFromString(String fileContents, @Nullable String configRoot) throws IOException {
        ConfigFinder finder = new ConfigFinder(configRoot);
        return finder.getConfig(fileContents);
    }
}

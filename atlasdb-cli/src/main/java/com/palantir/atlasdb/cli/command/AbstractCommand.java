/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.cli.command;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;

import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbConfigs;

import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

public abstract class AbstractCommand implements Callable<Integer> {
    public static final String ALTERNATE_ATLASDB_CONFIG_OBJECT_PATH = "/atlas";

    @Option(name = {"-c", "--config"},
            title = "CONFIG PATH",
            type = OptionType.GLOBAL,
            description = "path to yaml configuration file for atlasdb")
    private File configFile;

    // TODO(bgrabham): Hide this argument once https://github.com/airlift/airline/issues/51 is fixed
    @Option(name = {"--inline-config"},
            title = "INLINE CONFIG",
            type = OptionType.GLOBAL,
            description = "inline configuration file for atlasdb")
    private String inlineConfig;

    @Option(name = {"--config-root"},
            title = "CONFIG ROOT",
            type = OptionType.GLOBAL,
            description = "field in the config yaml file that contains the atlasdb configuration root")
    private String configRoot = AtlasDbConfigs.ATLASDB_CONFIG_OBJECT_PATH;

    @Option(name = {"--offline"},
            title = "OFFLINE",
            type = OptionType.GLOBAL,
            description = "run this cli offline")
    private boolean offline = false;

    private AtlasDbConfig config;

    protected AtlasDbConfig getAtlasDbConfig() {
        if (config == null) {
            try {
                if (configFile != null) {
                    config = parseAtlasDbConfig();
                } else if (inlineConfig != null) {
                    config = AtlasDbConfigs.loadFromString(inlineConfig, "");
                } else {
                    throw new IllegalArgumentException("Required option '-c' is missing");
                }
                if (offline) {
                    config = config.toOfflineConfig();
                }
            } catch (IOException e) {
                throw new RuntimeException(String.format("IOException thrown reading configuration file: %s",
                        configFile != null ? configFile.getPath() : "null"), e);
            }
        }

        return config;
    }

    private AtlasDbConfig parseAtlasDbConfig() {
        try {
            return AtlasDbConfigs.load(configFile, configRoot);
        } catch (Exception e) {
            try {
                return AtlasDbConfigs.load(configFile, ALTERNATE_ATLASDB_CONFIG_OBJECT_PATH);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to load the atlasdb config. One possibility"
                        + " is that the AtlasDB block root in the config is not '/atlasdb' nor '/atlas'."
                        + " You can specify a different config root by specifying the --config-root option"
                        + " before the command (i.e. sweep, migrate).",
                        ex);
            }
        }
    }

    protected boolean isOffline() {
        return offline;
    }
}

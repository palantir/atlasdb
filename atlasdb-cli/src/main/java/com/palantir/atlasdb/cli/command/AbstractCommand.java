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
package com.palantir.atlasdb.cli.command;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;

import com.google.common.base.Optional;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbConfigs;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;

import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

public abstract class AbstractCommand implements Callable<Integer> {
    @Option(name = {"-c", "--config"},
            title = "CONFIG PATH",
            type = OptionType.GLOBAL,
            description = "path to yaml configuration file for atlasdb")
    private File configFile;

    // TODO: Hide this argument once https://github.com/airlift/airline/issues/51 is fixed
    @Option(name = {"--inline-config"},
            title = "INLINE CONFIG",
            type = OptionType.GLOBAL,
            description = "inline configuration file for atlasdb")
    private String inlineConfig;

    @Option(name = {"--config-root"},
            title = "CONFIG ROOT",
            type = OptionType.GLOBAL,
            description = "field in the config yaml file that contains the atlasdb configuration root")
    private String configRoot = AtlasDbConfigs.ATLASDB_CONFIG_ROOT;

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
                    config = AtlasDbConfigs.load(configFile, configRoot);
                } else if (inlineConfig != null) {
                    config = AtlasDbConfigs.loadFromString(inlineConfig, "");
                } else {
                    throw new IllegalArgumentException("Required option '-c' is missing");
                }
                if (offline) {
                    config = ImmutableAtlasDbConfig.builder()
                            .from(config)
                            .leader(Optional.absent())
                            .lock(Optional.absent())
                            .timestamp(Optional.absent())
                            .build();
                }
            } catch (IOException e) {
                throw new RuntimeException(String.format("IOException thrown reading configuration file: %s",
                        configFile != null ? configFile.getPath() : "null"), e);
            }
        }

        return config;
    }

    protected boolean isOffline() {
        return offline;
    }
}

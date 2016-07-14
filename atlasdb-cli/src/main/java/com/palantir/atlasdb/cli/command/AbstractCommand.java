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

import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbConfigs;

import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

public abstract class AbstractCommand implements Callable<Integer> {

    @Option(name = {"-c", "--config"},
            title = "CONFIG PATH",
            type = OptionType.GLOBAL,
            description = "path to yaml configuration file for atlasdb",
            required = true)
    private File configFile;

    @Option(name = {"--config-root"},
            title = "CONFIG ROOT",
            type = OptionType.GLOBAL,
            description = "field in the config yaml file that contains the atlasdb configuration root")
    private String configRoot = AtlasDbConfigs.ATLASDB_CONFIG_ROOT;

    @Option(name = {"--root-is-path"},
            title = "CONFIG PATH",
            type = OptionType.GLOBAL,
            description = "if set, means the value specified by --config-root is a full path to the field, not just the field")
    private boolean isPath;

    private AtlasDbConfig config;
    
    protected AtlasDbConfig getAtlasDbConfig() {
        if (config == null) {
            try {
                config = AtlasDbConfigs.load(configFile, configRoot, isPath);
            } catch (IOException e) {
                throw new RuntimeException(String.format("IOException thrown reading configuration file: %s",
                        configFile.getPath()), e);
            }
        }
        
        return config;
    }

}

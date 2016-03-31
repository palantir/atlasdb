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

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.cli.services.AtlasDbServices;
import com.palantir.atlasdb.cli.services.AtlasDbServicesFactory;
import com.palantir.atlasdb.cli.services.DaggerAtlasDbServices;
import com.palantir.atlasdb.cli.services.ServicesConfigModule;
import com.palantir.common.base.Throwables;

import io.airlift.airline.Option;

public abstract class SingleBackendCommand implements Callable<Integer> {

    @Option(name = {"-c", "--config"},
            title = "CONFIG PATH",
            description = "path to yaml configuration file for atlasdb",
            required = true)
    private File configFile;

    @Option(name = {"--config-root"},
            title = "CONFIG ROOT",
            description = "field in the config yaml file that contains the atlasdb configuration root")
    private String configRoot = "atlasdb";

    @Override
    public Integer call() {
        try (AtlasDbServices services = connect()) {
            return execute(services);
        } catch (Exception e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    public abstract int execute(AtlasDbServices services);

    private AtlasDbServices connect() throws IOException {
        ServicesConfigModule scm = ServicesConfigModule.create(configFile, configRoot);
        return DaggerAtlasDbServices.builder().servicesConfigModule(scm).build();
    }

    @VisibleForTesting
    public <T extends AtlasDbServices> T connect(AtlasDbServicesFactory factory) throws IOException {
        ServicesConfigModule scm = ServicesConfigModule.create(configFile, configRoot);
        return factory.connect(scm);
    }

}

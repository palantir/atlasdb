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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.cli.services.AtlasDbServices;
import com.palantir.atlasdb.cli.services.AtlasDbServicesFactory;
import com.palantir.atlasdb.cli.services.DaggerAtlasDbServices;
import com.palantir.atlasdb.cli.services.ServicesConfigModule;

public abstract class SingleBackendCommand extends AbstractCommand {

    private static final Logger log = LoggerFactory.getLogger(SingleBackendCommand.class);

    @Override
    public Integer call() throws Exception {
        Preconditions.checkState(isOnlineRunSupported() || isOffline(), "This CLI can only be run offline");

        try {
            try (AtlasDbServices services = connect()) {
                return execute(services);
            }
        } catch (Throwable t) {
            System.err.println("FATAL: unhandled exception in execution of command");
            t.printStackTrace(System.err);
            log.error("FATAL: unhandled exception in execution of command", t);
            throw t;
        }
    }

    public abstract int execute(AtlasDbServices services);

    private AtlasDbServices connect() throws IOException {
        ServicesConfigModule scm = ServicesConfigModule.create(getAtlasDbConfig());
        return DaggerAtlasDbServices.builder().servicesConfigModule(scm).build();
    }

    @VisibleForTesting
    public <T extends AtlasDbServices> T connect(AtlasDbServicesFactory factory) throws IOException {
        return factory.connect(ServicesConfigModule.create(getAtlasDbConfig()));
    }

    public abstract boolean isOnlineRunSupported();
}

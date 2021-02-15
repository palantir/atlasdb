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
package com.palantir.atlasdb.cli.command;

import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.services.AtlasDbServicesFactory;
import com.palantir.atlasdb.services.DaggerAtlasDbServices;
import com.palantir.atlasdb.services.ServicesConfigModule;
import com.palantir.common.base.Throwables;
import com.palantir.logsafe.Preconditions;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SingleBackendCommand extends AbstractCommand {
    private static final Logger log = LoggerFactory.getLogger(SingleBackendCommand.class);

    @Override
    public Integer call() {
        Preconditions.checkState(isOnlineRunSupported() || isOffline(), "This CLI can only be run offline");

        try (AtlasDbServices services = connect()) {
            return execute(services);
        } catch (Exception e) {
            log.error("Exception encountered when running CLI", e);
            System.err.println(String.format("Exception encountered when running CLI: %s", e));
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    private AtlasDbServices connect() throws IOException {
        ServicesConfigModule scm = ServicesConfigModule.create(getAtlasDbConfig(), getAtlasDbRuntimeConfig());
        return DaggerAtlasDbServices.builder().servicesConfigModule(scm).build();
    }

    public <T extends AtlasDbServices> T connect(AtlasDbServicesFactory factory) throws IOException {
        return factory.connect(ServicesConfigModule.create(getAtlasDbConfig(), getAtlasDbRuntimeConfig()));
    }

    public abstract int execute(AtlasDbServices services);

    public abstract boolean isOnlineRunSupported();
}

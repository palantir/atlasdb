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

import java.io.IOException;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.services.AtlasDbServicesFactory;
import com.palantir.atlasdb.services.DaggerAtlasDbServices;
import com.palantir.atlasdb.services.ServicesConfigModule;
import com.palantir.common.base.Throwables;

public abstract class SingleBackendCommand extends AbstractCommand {

    @Override
    public Integer call() {
        Preconditions.checkState(isOnlineRunSupported() || isOffline(), "This CLI can only be run offline");

        try (AtlasDbServices services = connect()) {
            return execute(services);
        } catch (Exception e) {
            e.printStackTrace();
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    private AtlasDbServices connect() throws IOException {
        ServicesConfigModule scm = ServicesConfigModule.create(getAtlasDbConfig());
        return DaggerAtlasDbServices.builder().servicesConfigModule(scm).build();
    }

    public <T extends AtlasDbServices> T connect(AtlasDbServicesFactory factory) throws IOException {
        return factory.connect(ServicesConfigModule.create(getAtlasDbConfig()));
    }

    public abstract int execute(AtlasDbServices services);

    public abstract boolean isOnlineRunSupported();
}

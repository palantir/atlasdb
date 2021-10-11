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
package com.palantir.atlasdb.cli.runner;

import com.palantir.atlasdb.cli.command.SingleBackendCommand;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;

public class InMemoryTestRunner extends AbstractTestRunner<AtlasDbServices> {

    public static final String CONFIG_LOCATION = "cli_test_config.yml";

    public InMemoryTestRunner(Class<? extends SingleBackendCommand> cmdClass, String... args) {
        super(cmdClass, args);
    }

    @Override
    protected String getKvsConfigFileName() {
        return CONFIG_LOCATION;
    }

    @Override
    protected void cleanup(KeyValueServiceConfig kvsConfig) {
        // noop
    }
}

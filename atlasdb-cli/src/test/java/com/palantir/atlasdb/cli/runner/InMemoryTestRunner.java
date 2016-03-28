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
package com.palantir.atlasdb.cli.runner;

import com.palantir.atlasdb.cli.command.SingleBackendCommand;
import com.palantir.atlasdb.keyvalue.api.KeyValueServiceConfig;

public class InMemoryTestRunner extends AbstractTestRunner {

    public static final String INMEMORY_CONFIG_FILENAME = "inmemory_config.yml";

    public InMemoryTestRunner(Class<? extends SingleBackendCommand> cmdClass, String... args) {
        super(cmdClass, args);
    }

    @Override
    protected String getKvsConfigFileName() {
        return INMEMORY_CONFIG_FILENAME;
    }

    @Override
    protected void cleanup(KeyValueServiceConfig kvsConfig) {
        // noop
    }

}

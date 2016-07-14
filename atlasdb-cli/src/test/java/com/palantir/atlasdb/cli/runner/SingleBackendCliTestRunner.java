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

import java.net.URISyntaxException;

import com.palantir.atlasdb.cli.services.AtlasDbServices;
import com.palantir.atlasdb.cli.services.AtlasDbServicesFactory;

public interface SingleBackendCliTestRunner extends AutoCloseable {

    <T extends AtlasDbServices> T connect(AtlasDbServicesFactory factory) throws Exception;

    void parse(String... args);

    String run();

    String run(boolean failOnNonZeroExit, boolean singleLine);

    // Commands are often not idempotent.  Calling this will clear any state
    // held in the command class instance, but not the KVS
    void freshCommand() throws URISyntaxException;

}

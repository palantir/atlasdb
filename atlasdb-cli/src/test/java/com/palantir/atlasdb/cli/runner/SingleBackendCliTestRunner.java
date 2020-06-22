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

import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.services.AtlasDbServicesFactory;
import java.net.URISyntaxException;

public interface SingleBackendCliTestRunner extends AutoCloseable {

    <T extends AtlasDbServices> T connect(AtlasDbServicesFactory factory) throws Exception;

    void parse(String... args);

    String run();

    String run(boolean failOnNonZeroExit, boolean singleLine);

    // Commands are often not idempotent.  Calling this will clear any state
    // held in the command class instance, but not the KVS
    void freshCommand() throws URISyntaxException;

}

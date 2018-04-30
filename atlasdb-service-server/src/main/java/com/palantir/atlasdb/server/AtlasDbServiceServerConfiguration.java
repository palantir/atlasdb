/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.server;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.palantir.atlasdb.config.AtlasDbConfig;

import io.dropwizard.Configuration;

public class AtlasDbServiceServerConfiguration extends Configuration {

    private final AtlasDbConfig atlasdb;

    public AtlasDbServiceServerConfiguration(@JsonProperty("atlasdb") AtlasDbConfig atlasdb) {
        this.atlasdb = atlasdb;
    }

    public AtlasDbConfig getConfig() {
        return atlasdb;
    }

}

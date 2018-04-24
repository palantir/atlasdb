/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.timelock.config.DatabaseTsBoundPersisterConfiguration;
import com.palantir.timelock.config.TsBoundPersisterConfiguration;

public class TimestampBoundStoreConfigDeserializationTest extends AbstractTimelockServerConfigDeserializationTest {
    @Override
    public String getConfigFileName() {
        return "/timestampBoundTestConfig.yml";
    }

    @Override
    public void assertTimestampBoundPersisterConfigurationCorrect(TsBoundPersisterConfiguration configuration) {
        assertThat(configuration).isInstanceOf(DatabaseTsBoundPersisterConfiguration.class);
        DatabaseTsBoundPersisterConfiguration databaseTsBoundPersisterConfiguration =
                (DatabaseTsBoundPersisterConfiguration) configuration;

        assertThat(databaseTsBoundPersisterConfiguration.keyValueServiceConfig())
                .isEqualTo(new InMemoryAtlasDbConfig());
    }
}

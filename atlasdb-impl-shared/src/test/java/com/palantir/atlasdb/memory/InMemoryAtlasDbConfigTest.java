/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.memory;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.palantir.atlasdb.spi.KeyValueServiceConfigHelper;

public class InMemoryAtlasDbConfigTest {
    private static final InMemoryAtlasDbConfig CONFIG_1 = new InMemoryAtlasDbConfig();
    private static final InMemoryAtlasDbConfig CONFIG_2 = new InMemoryAtlasDbConfig();

    @Test
    public void twoDistinctInstancesOfInMemoryConfigsAreEqual() {
        assertThat(CONFIG_1).isEqualTo(CONFIG_2).isNotSameAs(CONFIG_2);
    }

    @Test
    public void twoInstancesOfInMemoryConfigsHaveEqualHashCodes() {
        assertThat(CONFIG_1.hashCode()).isEqualTo(CONFIG_2.hashCode());
    }

    @Test
    public void inMemoryConfigNotEqualToOtherKeyValueServiceConfig() {
        KeyValueServiceConfigHelper otherKvsConfig = () -> "FooDB";
        assertThat(CONFIG_1).isNotEqualTo(otherKvsConfig);
    }
}

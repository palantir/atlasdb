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
package com.palantir.atlasdb.memory;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.spi.KeyValueServiceConfigHelper;
import org.junit.Test;

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

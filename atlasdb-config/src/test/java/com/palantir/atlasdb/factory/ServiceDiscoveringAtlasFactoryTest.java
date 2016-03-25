/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.factory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import org.junit.Test;

import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;

public class ServiceDiscoveringAtlasFactoryTest {
    private final KeyValueServiceConfig kvsConfig = () -> DummyAtlasDbFactory.TYPE;
    private final AtlasDbFactory delegate = new DummyAtlasDbFactory();

    @Test
    public void delegateToFactoriesOnTheClasspath() {
        ServiceDiscoveringAtlasFactory factory = new ServiceDiscoveringAtlasFactory(kvsConfig);

        assertThat(factory.createKeyValueService(),
                equalTo(delegate.createRawKeyValueService(kvsConfig)));
    }
}

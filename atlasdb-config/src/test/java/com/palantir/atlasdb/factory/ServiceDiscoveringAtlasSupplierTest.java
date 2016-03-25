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
import static org.hamcrest.core.Is.is;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;

public class ServiceDiscoveringAtlasSupplierTest {
    private final KeyValueServiceConfig kvsConfig = () -> AutoServiceAnnotatedAtlasDbFactory.TYPE;
    private final KeyValueServiceConfig invalidKvsConfig = () -> "should not be found kvs";
    private final AtlasDbFactory delegate = new AutoServiceAnnotatedAtlasDbFactory();

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void delegateToFactoriesAnnotatedWithAutoServiceForCreatingKeyValueServices() {
        ServiceDiscoveringAtlasSupplier atlasSupplier = new ServiceDiscoveringAtlasSupplier(kvsConfig);

        assertThat(
                atlasSupplier.getKeyValueService(),
                is(delegate.createRawKeyValueService(kvsConfig)));
    }

    @Test
    public void delegateToFactoriesAnnotatedWithAutoServiceForCreatingTimestampServices() {
        ServiceDiscoveringAtlasSupplier atlasSupplier = new ServiceDiscoveringAtlasSupplier(kvsConfig);
        KeyValueService delegateKeyValueService = delegate.createRawKeyValueService(kvsConfig);

        assertThat(
                atlasSupplier.getTimestampService(),
                is(delegate.createTimestampService(delegateKeyValueService)));
    }

    @Test
    public void notAllowConstructionWithoutAValidBackingFactory() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("No atlas provider");
        exception.expectMessage(invalidKvsConfig.type());
        exception.expectMessage("Have you annotated it with @AutoService(AtlasDbFactory.class)?");

        new ServiceDiscoveringAtlasSupplier(invalidKvsConfig);
    }
}

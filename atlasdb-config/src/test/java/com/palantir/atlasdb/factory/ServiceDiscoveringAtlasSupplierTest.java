/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Optional;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfigHelper;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;

public class ServiceDiscoveringAtlasSupplierTest {
    private final KeyValueServiceConfigHelper kvsConfig = () -> AutoServiceAnnotatedAtlasDbFactory.TYPE;
    private final KeyValueServiceConfigHelper invalidKvsConfig = () -> "should not be found kvs";
    private final AtlasDbFactory delegate = new AutoServiceAnnotatedAtlasDbFactory();
    private final Optional<LeaderConfig> leaderConfig = Optional.of(mock(LeaderConfig.class));
    private final MetricsManager metrics = MetricsManagers.createForTests();

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void delegateToFactoriesAnnotatedWithAutoServiceForCreatingKeyValueServices() {
        ServiceDiscoveringAtlasSupplier atlasSupplier = new ServiceDiscoveringAtlasSupplier(
                metrics, kvsConfig, leaderConfig);

        assertThat(
                atlasSupplier.getKeyValueService(),
                is(delegate.createRawKeyValueService(metrics, kvsConfig, leaderConfig)));
    }

    @Test
    public void notAllowConstructionWithoutAValidBackingFactory() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("No atlas provider");
        exception.expectMessage(invalidKvsConfig.type());
        exception.expectMessage("Have you annotated it with @AutoService(AtlasDbFactory.class)?");

        new ServiceDiscoveringAtlasSupplier(metrics, invalidKvsConfig, leaderConfig);
    }

    @Test
    public void alwaysSaveThreadDumpsToTheSameFile() throws IOException {
        ServiceDiscoveringAtlasSupplier supplier = new ServiceDiscoveringAtlasSupplier(
                metrics, kvsConfig, leaderConfig);

        String firstPath = supplier.saveThreadDumps();
        String secondPath = supplier.saveThreadDumps();

        assertEquals(firstPath, secondPath);
    }

    private Matcher<Object> sameObjectAs(Object initial) {
        return new TypeSafeDiagnosingMatcher<Object>() {
            @Override
            protected boolean matchesSafely(Object item, Description mismatchDescription) {
                mismatchDescription.appendValue(item);
                return initial == item;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("Object which is exactly the same as ").appendValue(initial);

            }
        };
    }
}

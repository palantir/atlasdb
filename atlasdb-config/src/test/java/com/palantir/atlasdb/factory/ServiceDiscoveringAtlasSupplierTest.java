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
import static org.hamcrest.core.IsNot.not;
import static org.mockito.Mockito.mock;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.base.Optional;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.timestamp.TimestampService;

public class ServiceDiscoveringAtlasSupplierTest {
    private final KeyValueServiceConfig kvsConfig = () -> AutoServiceAnnotatedAtlasDbFactory.TYPE;
    private final KeyValueServiceConfig invalidKvsConfig = () -> "should not be found kvs";
    private final AtlasDbFactory delegate = new AutoServiceAnnotatedAtlasDbFactory();
    private final Optional<LeaderConfig> leaderConfig = Optional.of(mock(LeaderConfig.class));

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void delegateToFactoriesAnnotatedWithAutoServiceForCreatingKeyValueServices() {
        ServiceDiscoveringAtlasSupplier atlasSupplier = new ServiceDiscoveringAtlasSupplier(kvsConfig, leaderConfig);

        assertThat(
                atlasSupplier.getKeyValueService(),
                is(delegate.createRawKeyValueService(kvsConfig, leaderConfig.get())));
    }

    @Test
    public void delegateToFactoriesAnnotatedWithAutoServiceForCreatingTimestampServices() {
        ServiceDiscoveringAtlasSupplier atlasSupplier = new ServiceDiscoveringAtlasSupplier(kvsConfig, leaderConfig);
        TimestampService timestampService = mock(TimestampService.class);
        AutoServiceAnnotatedAtlasDbFactory.nextTimestampServiceToReturn(timestampService);

        assertThat(
                atlasSupplier.getTimestampService(),
                is(timestampService));
    }

    @Test
    public void notAllowConstructionWithoutAValidBackingFactory() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("No atlas provider");
        exception.expectMessage(invalidKvsConfig.type());
        exception.expectMessage("Have you annotated it with @AutoService(AtlasDbFactory.class)?");

        new ServiceDiscoveringAtlasSupplier(invalidKvsConfig, leaderConfig);
    }

    @Test
    public void returnDifferentTimestampServicesOnSubsequentCalls() {
        // Need to get a newly-initialized timestamp service in case leadership changed between calls.
        ServiceDiscoveringAtlasSupplier supplier = new ServiceDiscoveringAtlasSupplier(kvsConfig, leaderConfig);
        AutoServiceAnnotatedAtlasDbFactory.nextTimestampServiceToReturn(mock(TimestampService.class), mock(TimestampService.class));

        assertThat(supplier.getTimestampService(), is(not(sameObjectAs(supplier.getTimestampService()))));
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

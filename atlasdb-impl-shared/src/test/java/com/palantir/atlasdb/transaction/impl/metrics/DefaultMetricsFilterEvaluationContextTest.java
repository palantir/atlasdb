/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.metrics;

import static com.palantir.logsafe.testing.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.util.TopNMetricPublicationController;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("unchecked") // mocks
public class DefaultMetricsFilterEvaluationContextTest {
    private static final String KEY_1 = "skeleton";
    private static final String KEY_2 = "c#minor";
    private static final String KEY_3 = "oftheancients";

    private final Supplier<TopNMetricPublicationController<Long>> controllerFactory = mock(Supplier.class);
    private final DefaultMetricsFilterEvaluationContext context =
            new DefaultMetricsFilterEvaluationContext(controllerFactory);

    @Before
    public void setUp() {
        when(controllerFactory.get()).thenReturn(TopNMetricPublicationController.create(1));
    }

    @Test
    public void consistentByKey() {
        context.registerAndCreateTopNFilter(KEY_1, () -> 8L);
        context.registerAndCreateTopNFilter(KEY_1, () -> 4L);
        context.registerAndCreateTopNFilter(KEY_1, () -> 2L);
        assertThat(context.getRegisteredKeys()).containsExactlyInAnyOrder(KEY_1);
        verify(controllerFactory).get();
    }

    @Test
    public void maintainsSeparateKeys() {
        context.registerAndCreateTopNFilter(KEY_1, () -> 8L);
        context.registerAndCreateTopNFilter(KEY_2, () -> 4L);
        context.registerAndCreateTopNFilter(KEY_3, () -> 2L);
        assertThat(context.getRegisteredKeys()).containsExactlyInAnyOrder(KEY_1, KEY_2, KEY_3);
        verify(controllerFactory, times(3)).get();
    }
}

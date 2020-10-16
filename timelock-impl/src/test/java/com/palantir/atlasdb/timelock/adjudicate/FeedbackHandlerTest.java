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

package com.palantir.atlasdb.timelock.adjudicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.codahale.metrics.Gauge;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.timelock.feedback.ConjureTimeLockClientFeedback;
import com.palantir.tritium.metrics.registry.MetricName;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

public class FeedbackHandlerTest {
    private static final ConjureTimeLockClientFeedback FEEDBACK = ConjureTimeLockClientFeedback.builder()
            .atlasVersion("1.2.3")
            .serviceName("tom")
            .nodeId(UUID.randomUUID())
            .build();
    private static final MetricName METRIC_NAME = MetricName.builder()
            .safeName(TimeLockClientFeedbackSink.class.getName() + ".numberOfFeedbackReports")
            .build();

    private final TimeLockClientFeedbackSink sink = mock(TimeLockClientFeedbackSink.class);

    @Test
    public void putsFeedbackInSinkByDefault() {
        FeedbackHandler handler = new FeedbackHandler(sink, () -> true);
        handler.handle(FEEDBACK);
        verify(sink, times(1)).registerFeedback(FEEDBACK);
    }

    @Test
    public void doesNotPutFeedbackInSinkIfConfiguredNotTo() {
        FeedbackHandler handler = new FeedbackHandler(sink, () -> false);
        handler.handle(FEEDBACK);
        verify(sink, never()).registerFeedback(FEEDBACK);
    }

    @Test
    public void liveReloadsPublicationConfig() {
        AtomicBoolean atomicBoolean = new AtomicBoolean(true);
        FeedbackHandler handler = new FeedbackHandler(sink, atomicBoolean::get);
        handler.handle(FEEDBACK);
        verify(sink, times(1)).registerFeedback(FEEDBACK);

        atomicBoolean.set(false);
        handler.handle(FEEDBACK);
        verify(sink, times(1)).registerFeedback(FEEDBACK);

        atomicBoolean.set(true);
        handler.handle(FEEDBACK);
        verify(sink, times(2)).registerFeedback(FEEDBACK);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void publishesSizeMetricToMetricManager() {
        MetricsManager metricsManager = MetricsManagers.createForTests();
        FeedbackHandler handler = new FeedbackHandler(metricsManager, () -> true);

        handler.handle(FEEDBACK);
        assertThat(metricsManager.getPublishableMetrics().getMetrics())
                .containsKey(METRIC_NAME)
                .satisfies(map -> assertThat(((Gauge<Long>) map.get(METRIC_NAME)).getValue())
                        .isEqualTo(1L));
    }
}

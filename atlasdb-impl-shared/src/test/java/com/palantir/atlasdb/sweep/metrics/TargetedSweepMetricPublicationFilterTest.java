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

package com.palantir.atlasdb.sweep.metrics;

import static com.palantir.logsafe.testing.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

public class TargetedSweepMetricPublicationFilterTest {
    private final AtomicLong enqueuedWrites = new AtomicLong();
    private final AtomicLong entriesRead = new AtomicLong();
    private final AtomicLong millisSinceLastSweptTs = new AtomicLong();

    private final TargetedSweepMetricPublicationFilter filter = new TargetedSweepMetricPublicationFilter(
            ImmutableDecisionMetrics.builder()
                    .enqueuedWrites(enqueuedWrites::get)
                    .entriesRead(entriesRead::get)
                    .millisSinceLastSweptTs(millisSinceLastSweptTs::get)
                    .build());

    @Test
    public void shouldNotInitiallyBePublished() {
        assertThat(filter.shouldPublish()).isFalse();
    }

    @Test
    public void shouldNotBePublishedAfterSmallSweep() {
        entriesRead.addAndGet(10);
        enqueuedWrites.incrementAndGet();
        millisSinceLastSweptTs.set(Duration.ofMinutes(1).toMillis());

        assertThat(filter.shouldPublish()).isFalse();
    }

    @Test
    public void shouldBePublishedAfterReadingManyValues() {
        entriesRead.addAndGet(TargetedSweepMetricPublicationFilter.MINIMUM_READS_WRITES_TO_BE_CONSIDERED_ACTIVE);

        assertThat(filter.shouldPublish()).isTrue();
    }

    @Test
    public void shouldBePublishedAfterWritingManyValues() {
        enqueuedWrites.addAndGet(TargetedSweepMetricPublicationFilter.MINIMUM_READS_WRITES_TO_BE_CONSIDERED_ACTIVE);

        assertThat(filter.shouldPublish()).isTrue();
    }

    @Test
    public void shouldBePublishedIfFarBehind() {
        millisSinceLastSweptTs.set(TargetedSweepMetricPublicationFilter.MINIMUM_STALE_DURATION.toMillis());

        assertThat(filter.shouldPublish()).isTrue();
    }

    @Test
    public void continueToBePublishedIfWeCatchUp() {
        millisSinceLastSweptTs.set(TargetedSweepMetricPublicationFilter.MINIMUM_STALE_DURATION.toMillis());

        assertThat(filter.shouldPublish()).isTrue();

        millisSinceLastSweptTs.set(1);
        assertThat(filter.shouldPublish()).isTrue();
    }
}

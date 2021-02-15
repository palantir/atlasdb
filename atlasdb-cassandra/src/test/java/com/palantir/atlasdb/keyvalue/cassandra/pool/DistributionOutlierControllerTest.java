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

package com.palantir.atlasdb.keyvalue.cassandra.pool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Gauge;
import com.palantir.atlasdb.metrics.MetricPublicationFilter;
import com.palantir.common.streams.KeyedStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.junit.Test;

public class DistributionOutlierControllerTest {
    private static final double MIN_THRESHOLD = 0.1;
    private static final double MAX_THRESHOLD = 2.0;

    private final AtomicLong tick = new AtomicLong();
    private final Clock clock = new Clock() {
        @Override
        public long getTick() {
            return tick.get();
        }
    };

    private final DistributionOutlierController defaultController =
            new DistributionOutlierController(clock, MIN_THRESHOLD, MAX_THRESHOLD);

    @Test
    public void returnsNullWhenNoDistributionPresent() {
        assertThat(defaultController.getMeanGauge().getValue()).isEqualTo(null);
    }

    @Test
    public void returnsMeanOfOneValue() {
        defaultController.registerAndCreateFilter(() -> 10L);
        assertThat(defaultController.getMeanGauge().getValue()).isEqualTo(10.0);
    }

    @Test
    public void correctlyHandlesGaugesReturningNull() {
        defaultController.registerAndCreateFilter(() -> null);
        assertThat(defaultController.getMeanGauge().getValue()).isEqualTo(null);
    }

    @Test
    public void returnsMeanAvoidingNulls() {
        defaultController.registerAndCreateFilter(() -> null);
        defaultController.registerAndCreateFilter(() -> 10L);
        defaultController.registerAndCreateFilter(() -> 7L);
        defaultController.registerAndCreateFilter(() -> null);
        defaultController.registerAndCreateFilter(() -> 13L);
        assertThat(defaultController.getMeanGauge().getValue()).isEqualTo(10.0);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void doesNotRepeatedlyQueryUnderlyingGauges() {
        Gauge<Long> mockGauge = mock(Gauge.class);
        when(mockGauge.getValue()).thenReturn(5L);
        defaultController.registerAndCreateFilter(mockGauge);
        defaultController.getMeanGauge().getValue();
        defaultController.getMeanGauge().getValue();
        defaultController.getMeanGauge().getValue();

        verify(mockGauge, times(1)).getValue();
    }

    @Test
    public void queriesGaugesAgainAfterTimeInterval() {
        AtomicLong value = new AtomicLong(5);
        defaultController.registerAndCreateFilter(value::get);
        defaultController.getMeanGauge().getValue();

        value.set(8);
        tick.addAndGet(DistributionOutlierController.REFRESH_INTERVAL.toNanos() + 1);
        assertThat(defaultController.getMeanGauge().getValue()).isEqualTo(8L);
    }

    @Test
    public void doNotPublishIfAllGaugesSimilar() {
        Stream<Gauge<Long>> gauges = Stream.of(() -> 6L, () -> 7L, () -> 8L, () -> 10L);
        assertThat(registerAndCreatePublicationFilters(gauges).values())
                .noneMatch(MetricPublicationFilter::shouldPublish);
    }

    @Test
    public void reportsGaugesBelowToleranceWindow() {
        Gauge<Long> lowGauge = () -> 1L;
        Gauge<Long> otherGauge = () -> 100L;
        Gauge<Long> anotherGauge = () -> 100L;

        Map<Gauge<Long>, MetricPublicationFilter> filters =
                registerAndCreatePublicationFilters(Stream.of(lowGauge, otherGauge, anotherGauge));
        assertThat(filters.get(lowGauge).shouldPublish()).isTrue();
        assertThat(filters.get(otherGauge).shouldPublish()).isFalse();
        assertThat(filters.get(anotherGauge).shouldPublish()).isFalse();
    }

    @Test
    public void reportsGaugesAboveToleranceWindow() {
        Gauge<Long> highGauge = () -> 1000L;
        Gauge<Long> otherGauge = () -> 100L;
        Gauge<Long> anotherGauge = () -> 100L;

        Map<Gauge<Long>, MetricPublicationFilter> filters =
                registerAndCreatePublicationFilters(Stream.of(highGauge, otherGauge, anotherGauge));
        assertThat(filters.get(highGauge).shouldPublish()).isTrue();
        assertThat(filters.get(otherGauge).shouldPublish()).isFalse();
        assertThat(filters.get(anotherGauge).shouldPublish()).isFalse();
    }

    @Test
    @SuppressWarnings("unchecked") // Mocks
    public void gaugePublicationDecisions() {
        AtomicLong valueOne = new AtomicLong(1L);
        AtomicLong valueTwo = new AtomicLong(100L);
        AtomicLong valueThree = new AtomicLong(100L);

        Gauge<Long> gaugeOne = valueOne::get;
        Gauge<Long> gaugeTwo = valueTwo::get;
        Gauge<Long> gaugeThree = valueThree::get;

        Map<Gauge<Long>, MetricPublicationFilter> filters =
                registerAndCreatePublicationFilters(Stream.of(gaugeOne, gaugeTwo, gaugeThree));

        assertThat(defaultController.getMeanGauge().getValue()).isEqualTo(67.0);
        assertThat(filters.get(gaugeOne).shouldPublish()).isTrue();
        assertThat(filters.get(gaugeTwo).shouldPublish()).isFalse();
        assertThat(filters.get(gaugeThree).shouldPublish()).isFalse();

        valueOne.set(100L);
        valueThree.set(1L);
        tick.addAndGet(DistributionOutlierController.REFRESH_INTERVAL.toNanos() + 1);
        assertThat(defaultController.getMeanGauge().getValue()).isEqualTo(67.0);
        assertThat(filters.get(gaugeOne).shouldPublish()).isFalse();
        assertThat(filters.get(gaugeTwo).shouldPublish()).isFalse();
        assertThat(filters.get(gaugeThree).shouldPublish()).isTrue();

        valueThree.set(100L);
        tick.addAndGet(DistributionOutlierController.REFRESH_INTERVAL.toNanos() + 1);
        assertThat(defaultController.getMeanGauge().getValue()).isEqualTo(100.0);
        assertThat(filters.get(gaugeOne).shouldPublish()).isFalse();
        assertThat(filters.get(gaugeTwo).shouldPublish()).isFalse();
        assertThat(filters.get(gaugeThree).shouldPublish()).isFalse();
    }

    private Map<Gauge<Long>, MetricPublicationFilter> registerAndCreatePublicationFilters(
            Stream<Gauge<Long>> gaugeStream) {
        return KeyedStream.of(gaugeStream)
                .map(defaultController::registerAndCreateFilter)
                .collectToMap();
    }
}

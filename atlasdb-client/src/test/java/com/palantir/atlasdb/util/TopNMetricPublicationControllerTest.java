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

package com.palantir.atlasdb.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codahale.metrics.Gauge;
import com.palantir.atlasdb.metrics.MetricPublicationFilter;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.Test;

public class TopNMetricPublicationControllerTest {
    @Test
    public void canSelectivelyPublishGaugeResults() {
        AtomicLong value1 = new AtomicLong(42);
        AtomicLong value2 = new AtomicLong(157);

        Gauge<Long> gauge1 = value1::get;
        Gauge<Long> gauge2 = value2::get;

        TopNMetricPublicationController<Long> controller = TopNMetricPublicationController.create(1);
        MetricPublicationFilter filter1 = controller.registerAndCreateFilter(gauge1);
        MetricPublicationFilter filter2 = controller.registerAndCreateFilter(gauge2);

        assertThat(filter1.shouldPublish()).isFalse();
        assertThat(filter2.shouldPublish()).isTrue();
    }

    @Test
    public void canSelectivelyPublishMultipleGaugeResults() {
        List<Gauge<Long>> atomicLongs = LongStream.range(0, 100)
                .mapToObj(AtomicLong::new)
                .<Gauge<Long>>map(atomicLong -> atomicLong::get)
                .collect(Collectors.toList());

        TopNMetricPublicationController<Long> controller = TopNMetricPublicationController.create(7);

        List<MetricPublicationFilter> filtersInOrder =
                atomicLongs.stream().map(controller::registerAndCreateFilter).collect(Collectors.toList());

        assertThat(filtersInOrder.get(0).shouldPublish()).isFalse();
        assertThat(filtersInOrder.get(50).shouldPublish()).isFalse();
        assertThat(filtersInOrder.get(92).shouldPublish()).isFalse();
        assertThat(filtersInOrder.get(93).shouldPublish()).isTrue();
        assertThat(filtersInOrder.get(94).shouldPublish()).isTrue();
        assertThat(filtersInOrder.get(99).shouldPublish()).isTrue();
    }

    @Test
    public void eligibleGaugesChangeOverTime() {
        AtomicLong value1 = new AtomicLong(42);
        AtomicLong value2 = new AtomicLong(157);

        Gauge<Long> gauge1 = value1::get;
        Gauge<Long> gauge2 = value2::get;

        TopNMetricPublicationController<Long> controller =
                new TopNMetricPublicationController<>(Comparator.<Long>naturalOrder(), 1, Duration.ofNanos(1));
        MetricPublicationFilter filter1 = controller.registerAndCreateFilter(gauge1);
        MetricPublicationFilter filter2 = controller.registerAndCreateFilter(gauge2);

        assertThat(filter1.shouldPublish()).isFalse();
        assertThat(filter2.shouldPublish()).isTrue();

        value1.set(8888);
        assertThat(filter1.shouldPublish()).isTrue();
        assertThat(filter2.shouldPublish()).isFalse();
    }

    @Test
    public void publishesAllGaugesIfThereAreFewerThanTheThreshold() {
        AtomicLong value1 = new AtomicLong(42);
        AtomicLong value2 = new AtomicLong(157);

        Gauge<Long> gauge1 = value1::get;
        Gauge<Long> gauge2 = value2::get;

        TopNMetricPublicationController<Long> controller = TopNMetricPublicationController.create(50);
        MetricPublicationFilter filter1 = controller.registerAndCreateFilter(gauge1);
        MetricPublicationFilter filter2 = controller.registerAndCreateFilter(gauge2);

        assertThat(filter1.shouldPublish()).isTrue();
        assertThat(filter2.shouldPublish()).isTrue();
    }

    @Test
    public void selectsArbitraryElementFromTies() {
        AtomicLong value1 = new AtomicLong(42);
        AtomicLong value2a = new AtomicLong(66);
        AtomicLong value2b = new AtomicLong(66);
        AtomicLong value3 = new AtomicLong(89);

        TopNMetricPublicationController<Long> controller = TopNMetricPublicationController.create(2);

        MetricPublicationFilter filter1 = controller.registerAndCreateFilter(value1::get);
        MetricPublicationFilter filter2a = controller.registerAndCreateFilter(value2a::get);
        MetricPublicationFilter filter2b = controller.registerAndCreateFilter(value2b::get);
        MetricPublicationFilter filter3 = controller.registerAndCreateFilter(value3::get);

        assertThat(filter1.shouldPublish()).isFalse();
        assertThat(filter2a.shouldPublish() ^ filter2b.shouldPublish()).isTrue();
        assertThat(filter3.shouldPublish()).isTrue();
    }

    @Test
    public void throwsWhenCreatingControllerWithNonPositiveArguments() {
        assertThatThrownBy(() -> TopNMetricPublicationController.create(0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("maxPermittedRank must be positive");
        assertThatThrownBy(() -> TopNMetricPublicationController.create(-29359))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("maxPermittedRank must be positive");
    }
}

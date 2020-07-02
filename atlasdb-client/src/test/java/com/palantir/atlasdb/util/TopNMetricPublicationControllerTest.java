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

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.junit.Test;

import com.codahale.metrics.Gauge;
import com.palantir.atlasdb.metrics.MetricPublicationFilter;

public class TopNMetricPublicationControllerTest {
    @Test
    public void orderStatisticOfNothingIsAbsent() {
        assertThat(TopNMetricPublicationController.calculateOrderStatistic(
                Stream.<Integer>of(),
                Comparator.naturalOrder(),
                1)).isEmpty();
        assertThat(TopNMetricPublicationController.calculateOrderStatistic(
                Stream.<Integer>of(null, null),
                Comparator.naturalOrder(),
                1)).isEmpty();
    }

    @Test
    public void calculatesSecondElementCorrectly() {
        assertThat(TopNMetricPublicationController.calculateOrderStatistic(
                IntStream.rangeClosed(1, 50).boxed(),
                Comparator.naturalOrder(),
                2)).contains(49);
    }

    @Test
    public void handlesDuplicates() {
        assertThat(TopNMetricPublicationController.calculateOrderStatistic(
                Stream.of(8, 8, 8, 8, 8),
                Comparator.naturalOrder(),
                2)).contains(8);
    }

    @Test
    public void orderStatisticAtEdgesOfStreamCanBeRetrieved() {
        assertThat(TopNMetricPublicationController.calculateOrderStatistic(
                IntStream.rangeClosed(1, 50).boxed(),
                Comparator.naturalOrder(),
                50)).contains(1);
        assertThat(TopNMetricPublicationController.calculateOrderStatistic(
                IntStream.rangeClosed(1, 50).boxed(),
                Comparator.naturalOrder(),
                51)).isEmpty();
    }

    @Test
    public void skipsNullsInOrderStatisticComputation() {
        assertThat(TopNMetricPublicationController.calculateOrderStatistic(
                Stream.of(null, null, 3, 7),
                Comparator.naturalOrder(),
                2)).contains(3);
        assertThat(TopNMetricPublicationController.calculateOrderStatistic(
                Stream.of(null, null, 3, 7),
                Comparator.naturalOrder(),
                3)).isEmpty();
    }

    @Test
    public void canSpecifyCustomComparator() {
        assertThat(TopNMetricPublicationController.calculateOrderStatistic(
                Stream.of("a", "bcd", "efghi", "jk"),
                Comparator.comparingInt(String::length),
                1)).contains("efghi");
    }

    @Test
    public void throwsIfAttemptingToRetrieveNonPositiveOrderStatistics() {
        assertThatThrownBy(() -> TopNMetricPublicationController.calculateOrderStatistic(
                IntStream.rangeClosed(1, 50).boxed(),
                Comparator.naturalOrder(),
                0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("The order statistic to be queried for must be positive");
        assertThatThrownBy(() -> TopNMetricPublicationController.calculateOrderStatistic(
                IntStream.rangeClosed(1, 50).boxed(),
                Comparator.naturalOrder(),
                -1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("The order statistic to be queried for must be positive");
    }

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

        List<MetricPublicationFilter> filtersInOrder = atomicLongs.stream()
                .map(controller::registerAndCreateFilter)
                .collect(Collectors.toList());

        assertThat(filtersInOrder.get(0).shouldPublish()).isFalse();
        assertThat(filtersInOrder.get(50).shouldPublish()).isFalse();
        assertThat(filtersInOrder.get(92).shouldPublish()).isFalse();
        assertThat(filtersInOrder.get(93).shouldPublish()).isTrue();
        assertThat(filtersInOrder.get(94).shouldPublish()).isTrue();
        assertThat(filtersInOrder.get(99).shouldPublish()).isTrue();
    }
}

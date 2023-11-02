/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

public class MetricNameUtilsTest {
    private static final String TAG_KEY_1 = "tag1";
    private static final String TAG_VALUE_1 = "tagVal1";
    private static final String METRIC_NAME = "metricName";
    private static final String TAG_VALUE_2 = "tagVal2";
    private static final String TAG_KEY_2 = "tag2";

    @Test
    public void shouldReturnNameWithSingleTag() {
        String metricName = MetricNameUtils.getMetricName(METRIC_NAME, ImmutableMap.of(TAG_KEY_1, TAG_VALUE_1));
        assertThat(metricName).isEqualTo("metricName;tag1=tagVal1");
    }

    @Test
    public void shouldReturnNameWithMultipleTags() {
        String metricName = MetricNameUtils.getMetricName(
                METRIC_NAME, ImmutableMap.of(TAG_KEY_2, TAG_VALUE_2, TAG_KEY_1, TAG_VALUE_1));
        assertThat(metricName).isIn("metricName;tag1=tagVal1;tag2=tagVal2", "metricName;tag2=tagVal2;tag1=tagVal1");
    }

    @Test
    public void shouldThrowIfMetricNameContainsSemiColon() {
        assertThatThrownBy(() -> MetricNameUtils.getMetricName(
                        "metric;Name", ImmutableMap.of(TAG_KEY_1, TAG_VALUE_1, TAG_KEY_2, TAG_VALUE_2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The metric name: metric;Name contains the forbidden character: ;");
    }

    @Test
    public void shouldThrowIfMetricNameContainsEqualSign() {
        assertThatThrownBy(() -> MetricNameUtils.getMetricName(
                        "metric=Name", ImmutableMap.of(TAG_KEY_1, TAG_VALUE_1, TAG_KEY_2, TAG_VALUE_2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The metric name: metric=Name contains the forbidden character: =");
    }

    @Test
    public void shouldThrowIfFirstMetricTagKeyContainsSemiColon() {
        assertThatThrownBy(() -> MetricNameUtils.getMetricName(
                        METRIC_NAME, ImmutableMap.of("tag;1", TAG_VALUE_1, TAG_KEY_2, TAG_VALUE_2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The tag key name: tag;1 contains the forbidden character: ;");
    }

    @Test
    public void shouldThrowIfFirstMetricTagValueContainsSemiColon() {
        assertThatThrownBy(() -> MetricNameUtils.getMetricName(
                        METRIC_NAME, ImmutableMap.of(TAG_KEY_1, "tag;Val1", TAG_KEY_2, TAG_VALUE_2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The tag value name: tag;Val1 contains the forbidden character: ;");
    }

    @Test
    public void shouldThrowIfFirstMetricTagKeyContainsEquals() {
        assertThatThrownBy(() -> MetricNameUtils.getMetricName(
                        METRIC_NAME, ImmutableMap.of("tag=1", TAG_VALUE_1, TAG_KEY_2, TAG_VALUE_2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The tag key name: tag=1 contains the forbidden character: =");
    }

    @Test
    public void shouldThrowIfFirstMetricTagValueContainsEquals() {
        assertThatThrownBy(() -> MetricNameUtils.getMetricName(
                        METRIC_NAME, ImmutableMap.of(TAG_KEY_1, "tag=Val1", TAG_KEY_2, TAG_VALUE_2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The tag value name: tag=Val1 contains the forbidden character: =");
    }

    @Test
    public void shouldThrowIfSecondMetricTagValueContainsEquals() {
        assertThatThrownBy(() -> MetricNameUtils.getMetricName(
                        METRIC_NAME, ImmutableMap.of(TAG_KEY_1, TAG_VALUE_1, TAG_KEY_2, "tagVal=2")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The tag value name: tagVal=2 contains the forbidden character: =");
    }

    @Test
    public void shouldThrowForFirstEncounteredErrorMultipleMetricArgsContainInvalidCharacters() {
        assertThatThrownBy(() -> MetricNameUtils.getMetricName(
                        "metric;Name", ImmutableMap.of("tag;1", TAG_VALUE_1, TAG_KEY_2, "tagVal=2")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The metric name: metric;Name contains the forbidden character: ;");
    }

    @Test
    public void shouldReturnANameIfTryingToRegisterMetricWithExactlyTenTags() {
        Map<String, String> tags =
                IntStream.range(0, 10).boxed().collect(Collectors.toMap(i -> "tag" + i, i -> "tagVal" + i));
        assertThat(tags).hasSize(10);
        assertThat(MetricNameUtils.getMetricName(METRIC_NAME, tags)).contains(METRIC_NAME);
    }

    @Test
    public void shouldThrowIfTryingToRegisterMetricWithMoreThanTenTags() {
        Map<String, String> tags =
                IntStream.range(0, 11).boxed().collect(Collectors.toMap(i -> "tag" + i, i -> "tagVal" + i));
        assertThat(tags).hasSize(11);
        assertThatThrownBy(() -> MetricNameUtils.getMetricName(METRIC_NAME, tags))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Too many tags set on the metric metricName. "
                        + "Maximum allowed number of tags is 10, found 11.");
    }
}

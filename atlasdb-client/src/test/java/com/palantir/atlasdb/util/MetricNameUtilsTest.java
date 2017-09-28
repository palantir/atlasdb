/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.util;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class MetricNameUtilsTest {

    @Test
    public void shouldReturnNameWithSingleTag() throws Exception {
        String metricName = MetricNameUtils.getMetricName("metricName", ImmutableMap.of("tag1", "tagVal1"));
        assertThat(metricName).isEqualTo("metricName;tag1=tagVal1");
    }

    @Test
    public void shouldReturnNameWithMultipleTags() throws Exception {
        String metricName = MetricNameUtils.getMetricName("metricName", ImmutableMap.of("tag1", "tagVal1", "tag2", "tagVal2"));
        assertThat(metricName).isEqualTo("metricName;tag1=tagVal1;tag2=tagVal2");
    }

    @Test
    public void shouldThrowIfSameTagIsAddedMultipleTimes() throws Exception {
        assertThatThrownBy(() -> MetricNameUtils.getMetricName("metricName", ImmutableMap.of("tag1", "tagVal1", "tag1", "tagVal2")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

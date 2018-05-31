/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

import java.util.Comparator;

import org.junit.Test;

public class AggregateRecomputingMetricTest {
    @Test
    public void canUpdate() throws InterruptedException {
        AggregateRecomputingMetric test = new AggregateRecomputingMetric(
                col -> col.stream().max(Comparator.naturalOrder()).orElse(0L), 1);

        assertThat(test.getValue()).isEqualTo(0L);

        test.update(1, 1L);
        Thread.sleep(2);

        assertThat(test.getValue()).isEqualTo(1L);

        test.update(2, 100L);
        Thread.sleep(2);

        assertThat(test.getValue()).isEqualTo(100L);

        test.update(2, 0L);
        Thread.sleep(2);
        assertThat(test.getValue()).isEqualTo(1L);
    }
}

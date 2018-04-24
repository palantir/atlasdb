/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.util.math;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class DistributionsTest {
    @Test
    public void universalDistributionConfidenceAlmostUniformDistributionTest() {
        List<Long> almostUniformDistribution = ImmutableList.of(199909L, 200065L, 199907L, 200070L, 199949L);
        Assertions.assertThat(Distributions.confidenceThatDistributionIsNotUniform(almostUniformDistribution))
                .isCloseTo(0.0, Offset.offset(0.01));
    }

    @Test
    public void universalDistributionConfidenceRelativelyUniformDistributionTest() {
        List<Long> relativelyUniformDistribution = ImmutableList.of(199809L, 200665L, 199607L, 200270L, 199649L);
        Assertions.assertThat(Distributions.confidenceThatDistributionIsNotUniform(relativelyUniformDistribution))
                .isLessThan(0.7);
    }

    @Test
    public void universalDistributionConfidenceSomeWhatSkewedDistributionTest() {
        List<Long> someWhatSkewedDistribution = ImmutableList.of(199609L, 200965L, 199607L, 200270L, 199649L);
        Assertions.assertThat(Distributions.confidenceThatDistributionIsNotUniform(someWhatSkewedDistribution))
                .isLessThan(0.88);
    }

    @Test
    public void universalDistributionConfidenceCompletelySkewedDistributionTest() {
        List<Long> completelySkewedDistribution = ImmutableList.of(522573L, 244456L, 139979L, 71531L, 21461L);
        Assertions.assertThat(Distributions.confidenceThatDistributionIsNotUniform(completelySkewedDistribution))
                .isCloseTo(1.0, Offset.offset(0.000001));
    }
}

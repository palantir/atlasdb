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

package com.palantir.util.math;

import java.util.List;

import org.apache.commons.math3.special.Gamma;

public final class Distributions {
    private Distributions() {
        // Utility class
    }

    public static double confidenceThatDistributionIsNotUniform(List<Long> values) {
        return 1.0 - confidenceThatDistributionIsUniform(values);
    }

    public static double confidenceThatDistributionIsUniform(List<Long> values) {
        return Gamma.regularizedGammaQ((values.size() - 1.0) / 2,
                chiSquareDistance(values) / 2);
    }

    private static double chiSquareDistance(List<Long> values) {
        double mean = values.stream().mapToLong(x -> x).reduce(0L, Long::sum) / (double) values.size();
        double distances =  values.stream().mapToDouble(Long::doubleValue)
                .reduce(0.0, (acc, nextVal) -> acc + Math.pow(nextVal - mean, 2));
        return distances / mean;
    }
}

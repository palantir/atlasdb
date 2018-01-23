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

package com.palantir.atlasdb.qos.config;

import org.immutables.value.Value;

import com.google.common.base.Preconditions;

@Value.Immutable
public abstract class CassandraHealthMetricMeasurement {
    abstract double lowerLimit();
    abstract double upperLimit();
    abstract Object currentValue();

    @Value.Check
    public void check() {
        Preconditions.checkState(lowerLimit() <= upperLimit(),
                "Lower limit should be less than or equal to the upper limit. Found LowerLimit: %s and UpperLimit: %s.",
                lowerLimit(), upperLimit());
    }

    boolean isMeasurementWithinLimits() {
        double measurement;
        try {
            measurement = Double.valueOf(currentValue().toString());
        } catch (ClassCastException ex) {
            // TODO(hsaraogi): The metric is not a number, should we throw here?
            return true;
        }
        return lowerLimit() <= measurement && measurement <= upperLimit();
    }
}

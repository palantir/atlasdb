/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.expectations;

import com.palantir.atlasdb.transaction.expectations.ExpectationsAlertingMetrics;
import com.palantir.atlasdb.util.CurrentValueMetric;
import com.palantir.atlasdb.util.MetricsManager;

final class GaugesForExpectationsAlertingMetrics {
    private final CurrentValueMetric<Boolean> ranForTooLong = new CurrentValueMetric<>();
    private final CurrentValueMetric<Boolean> readTooMuch = new CurrentValueMetric<>();
    private final CurrentValueMetric<Boolean> readTooMuchInOneKvsCall = new CurrentValueMetric<>();
    private final CurrentValueMetric<Boolean> queriedKvsTooMuch = new CurrentValueMetric<>();

    GaugesForExpectationsAlertingMetrics(MetricsManager metricsManager) {
        ExpectationsAlertingMetrics metrics = ExpectationsAlertingMetrics.of(metricsManager.getTaggedRegistry());
        metrics.ranForTooLong(ranForTooLong);
        metrics.readTooMuch(readTooMuch);
        metrics.readTooMuchInOneKvsCall(readTooMuchInOneKvsCall);
        metrics.queriedKvsTooMuch(queriedKvsTooMuch);
    }

    void updateRanForTooLong(boolean value) {
        ranForTooLong.setValue(value);
    }

    void updateReadTooMuch(boolean value) {
        readTooMuch.setValue(value);
    }

    void updateReadTooMuchInOneKvsCall(boolean value) {
        readTooMuchInOneKvsCall.setValue(value);
    }

    void updateQueriedKvsTooMuch(boolean value) {
        queriedKvsTooMuch.setValue(value);
    }
}

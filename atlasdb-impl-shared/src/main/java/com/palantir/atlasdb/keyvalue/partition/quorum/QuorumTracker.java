/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.partition.quorum;

import java.util.Map;
import java.util.concurrent.Future;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters.QuorumRequestParameters;
import com.palantir.common.collect.Maps2;

public class QuorumTracker<FutureReturnT, TrackingUnitT> {

    private final Map<TrackingUnitT, Integer> numberOfRemainingSuccessesForSuccess;
    private final Map<TrackingUnitT, Integer> numberOfRemainingFailuresForFailure;
    private final Map<Future<FutureReturnT>, Iterable<TrackingUnitT>> unitsByReference;
    private boolean failure;

    public QuorumTracker(Iterable<TrackingUnitT> allTrackedUnits,
                         QuorumRequestParameters quorumRequestParameters) {
        numberOfRemainingFailuresForFailure = Maps2.createConstantValueMap(
                allTrackedUnits,
                quorumRequestParameters.getFailureFactor());
        numberOfRemainingSuccessesForSuccess = Maps2.createConstantValueMap(
                allTrackedUnits,
                quorumRequestParameters.getSuccessFactor());
        unitsByReference = Maps.newHashMap();
        failure = false;
    }

    public QuorumTracker(Iterable<TrackingUnitT> allTrackedUnits,
                         Map<TrackingUnitT, QuorumRequestParameters> quorumRequestParameters) {
        numberOfRemainingFailuresForFailure = Maps.newHashMap(Maps.transformValues(quorumRequestParameters,
                input -> input.getFailureFactor()));
        numberOfRemainingSuccessesForSuccess = Maps.newHashMap(Maps.transformValues(quorumRequestParameters,
                input -> input.getSuccessFactor()));
        Preconditions.checkArgument(numberOfRemainingFailuresForFailure.keySet()
                .equals(Sets.newHashSet(allTrackedUnits)));
        Preconditions.checkArgument(numberOfRemainingSuccessesForSuccess.keySet()
                .equals(Sets.newHashSet(allTrackedUnits)));
        unitsByReference = Maps.newHashMap();
        failure = false;
    }

    /**
     * Deprecated.
     * @deprecated Use {@link #of(Iterable, Map)} instead
     */
    @Deprecated
    public static <FutureReturnTypeT, TrackingUnitT> QuorumTracker<FutureReturnTypeT, TrackingUnitT>
            of(Iterable<TrackingUnitT> allTrackedUnits,
                    QuorumRequestParameters quorumRequestParameters) {
        return new QuorumTracker<>(
                allTrackedUnits,
                quorumRequestParameters);
    }

    public static <FutureReturnTypeT, TrackingUnitT> QuorumTracker<FutureReturnTypeT, TrackingUnitT>
            of(Iterable<TrackingUnitT> allTrackedUnits,
                    Map<TrackingUnitT, QuorumRequestParameters> quorumRequestParameters) {
        return new QuorumTracker<>(allTrackedUnits, quorumRequestParameters);
    }

    public void handleSuccess(Future<FutureReturnT> ref) {
        Preconditions.checkState(!finished());
        Preconditions.checkArgument(unitsByReference.containsKey(ref));

        for (TrackingUnitT unit : unitsByReference.get(ref)) {
            if (numberOfRemainingSuccessesForSuccess.containsKey(unit)) {
                int newValue = numberOfRemainingSuccessesForSuccess.get(unit) - 1;
                if (newValue == 0) {
                    numberOfRemainingSuccessesForSuccess.remove(unit);
                    numberOfRemainingFailuresForFailure.remove(unit);
                } else {
                    numberOfRemainingSuccessesForSuccess.put(unit, newValue);
                }
            }
        }
        unregisterRef(ref);
    }

    public void handleFailure(Future<FutureReturnT> ref) {
        Preconditions.checkState(!finished());
        Preconditions.checkArgument(unitsByReference.containsKey(ref));
        for (TrackingUnitT cell : unitsByReference.get(ref)) {
            if (numberOfRemainingFailuresForFailure.containsKey(cell)) {
                int newValue = numberOfRemainingFailuresForFailure.get(cell) - 1;
                if (newValue == 0) {
                    failure = true;
                    break;
                } else {
                    numberOfRemainingFailuresForFailure.put(cell, newValue);
                }
            }
        }
        unregisterRef(ref);
    }

    public void registerRef(Future<FutureReturnT> ref, Iterable<TrackingUnitT> items) {
        Preconditions.checkState(!finished());
        unitsByReference.put(ref, items);
    }

    public void unregisterRef(Future<FutureReturnT> ref) {
        Preconditions.checkArgument(unitsByReference.containsKey(ref));
        unitsByReference.remove(ref);
    }

    public void cancel(boolean mayInterruptIfRunning) {
        for (Future<FutureReturnT> f : unitsByReference.keySet()) {
            f.cancel(mayInterruptIfRunning);
        }
    }

    public boolean failed() {
        return failure;
    }

    public boolean succeeded() {
        return !failed() && numberOfRemainingSuccessesForSuccess.isEmpty();
    }

    public boolean finished() {
        return failed() || succeeded();
    }

    public boolean hasJobsRunning() {
        return !unitsByReference.isEmpty();
    }
}

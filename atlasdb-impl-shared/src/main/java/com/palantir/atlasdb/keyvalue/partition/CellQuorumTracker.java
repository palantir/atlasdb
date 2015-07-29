package com.palantir.atlasdb.keyvalue.partition;

import java.util.Map;
import java.util.concurrent.Future;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.partition.QuorumParameters.QuorumRequestParameters;

public class CellQuorumTracker <T, U> {

    private final Map<U, Integer> numberOfRemainingSuccessesForSuccess;
    private final Map<U, Integer> numberOfRemainingFailuresForFailure;
    private final Map<Future<T>, Iterable<U>> cellsByReference;
    private boolean failure;

    /*
     * successFactor - minimum number of successes per cell
     */
    CellQuorumTracker(Iterable<U> allUs, QuorumRequestParameters qrp) {
        numberOfRemainingFailuresForFailure = Maps.newHashMap();
        numberOfRemainingSuccessesForSuccess = Maps.newConcurrentMap();

        for (U cell : allUs) {
            numberOfRemainingSuccessesForSuccess.put(cell, qrp.getSuccessFactor());
            numberOfRemainingFailuresForFailure.put(cell, qrp.getFailureFactor());
        }

        cellsByReference = Maps.newHashMap();
        failure = false;
    }

    static <V, W> CellQuorumTracker<V, W> of(Iterable<W> allUs, QuorumRequestParameters qrp) {
        return new CellQuorumTracker<V, W>(allUs, qrp);
    }

    void handleSuccess(Future<T> ref) {
        Preconditions.checkState(failure() == false && success() == false);
        Preconditions.checkArgument(cellsByReference.containsKey(ref));
        for (U cell : cellsByReference.get(ref)) {
            if (numberOfRemainingSuccessesForSuccess.containsKey(cell)) {
                int newValue = numberOfRemainingSuccessesForSuccess.get(cell) - 1;
                if (newValue == 0) {
                    numberOfRemainingSuccessesForSuccess.remove(cell);
                    numberOfRemainingFailuresForFailure.remove(cell);
                } else {
                    numberOfRemainingSuccessesForSuccess.put(cell, newValue);
                }
            }
        }
        cellsByReference.remove(ref);
    }

    void handleFailure(Future<T> ref) {
        Preconditions.checkState(failure() == false && success() == false);
        Preconditions.checkArgument(cellsByReference.containsKey(ref));
        for (U cell : cellsByReference.get(ref)) {
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
        cellsByReference.remove(ref);
    }

    void registerRef(Future<T> ref, Iterable<U> cells) {
        Preconditions.checkState(failure() == false && success() == false);
        cellsByReference.put(ref, cells);
    }

    void cancel(boolean mayInterruptIfRunning) {
        for (Future<T> f : cellsByReference.keySet()) {
            f.cancel(mayInterruptIfRunning);
        }
    }

    boolean failure() {
        return failure;
    }

    boolean success() {
        return !failure() && numberOfRemainingSuccessesForSuccess.isEmpty();
    }

    boolean finished() {
        return failure() || success();
    }
}
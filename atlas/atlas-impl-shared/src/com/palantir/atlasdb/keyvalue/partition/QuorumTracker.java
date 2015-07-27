package com.palantir.atlasdb.keyvalue.partition;

import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

class QuorumTracker <T, U> {

    private final Map<U, Integer> numberOfRemainingSuccessesForSuccess;
    private final Map<U, Integer> numberOfRemainingFailuresForFailure;
    private final Map<T, Iterable<U>> cellsByReference;
    private boolean failure;

    /*
     * successFactor - minimum number of successes per cell
     */
    QuorumTracker(Iterable<U> allUs, final int replicationFactor, final int successFactor) {
        numberOfRemainingFailuresForFailure = Maps.newHashMap();
        numberOfRemainingSuccessesForSuccess = Maps.newConcurrentMap();

        for (U cell : allUs) {
            numberOfRemainingFailuresForFailure.put(cell, replicationFactor - successFactor);
            numberOfRemainingSuccessesForSuccess.put(cell, successFactor);
        }

        cellsByReference = Maps.newHashMap();
        failure = false;
    }

    static <V, W> QuorumTracker<V, W> of(Iterable<W> allUs, final int replicationFactor, final int successFactor) {
        return new QuorumTracker<V, W>(allUs, replicationFactor, successFactor);
    }

    void handleSuccess(T ref) {
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
    }

    void handleFailure(T ref) {
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
    }

    void registerRef(T ref, Iterable<U> cells) {
        Preconditions.checkState(failure() == false && success() == false);
        cellsByReference.put(ref, cells);
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
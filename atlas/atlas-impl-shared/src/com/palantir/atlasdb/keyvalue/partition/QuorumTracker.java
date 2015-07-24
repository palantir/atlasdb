package com.palantir.atlasdb.keyvalue.partition;

import java.util.Map;
import java.util.concurrent.Future;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.Cell;

class QuorumTracker<FutureReturnType> {

    private final Map<Cell, Integer> numberOfRemainingSuccessesForSuccess;
    private final Map<Cell, Integer> numberOfRemainingFailuresForFailure;
    private final Map<Future<FutureReturnType>, Iterable<Cell>> futureToCells;
    private boolean failure;

    /*
     * successFactor - minimum number of successes per cell
     */
    QuorumTracker(Iterable<Cell> allCells, final int replicationFactor, final int successFactor) {
        numberOfRemainingFailuresForFailure = Maps.newHashMap();
        numberOfRemainingSuccessesForSuccess = Maps.newConcurrentMap();
        futureToCells = Maps.newHashMap();

        for (Cell cell : allCells) {
            numberOfRemainingFailuresForFailure.put(cell, replicationFactor - successFactor);
            numberOfRemainingSuccessesForSuccess.put(cell, successFactor);
        }

        failure = false;
    }

    static <T> QuorumTracker<T> of(Iterable<Cell> allCells, final int replicationFactor, final int successFactor) {
        return new QuorumTracker<T>(allCells, replicationFactor, successFactor);
    }

    void handleSuccess(Future<FutureReturnType> future) {
        Preconditions.checkArgument(futureToCells.containsKey(future));
        Preconditions.checkState(failure == false);
        Iterable<Cell> cellsThatSucceeded = futureToCells.get(future);
        for (Cell cell : cellsThatSucceeded) {
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

    void handleFailure(Future<FutureReturnType> future) {
        Preconditions.checkArgument(futureToCells.containsKey(future));
        Preconditions.checkState(failure == false);
        for (Cell cell : futureToCells.get(future)) {
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

    void registerFuture(Future<FutureReturnType> future, Iterable<Cell> cells) {
        futureToCells.put(future, cells);
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
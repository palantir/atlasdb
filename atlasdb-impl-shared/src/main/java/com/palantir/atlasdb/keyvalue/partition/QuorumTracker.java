package com.palantir.atlasdb.keyvalue.partition;

import java.util.Map;
import java.util.concurrent.Future;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.partition.QuorumParameters.QuorumRequestParameters;

public class QuorumTracker <T, U> {

    private final Map<U, Integer> numberOfRemainingSuccessesForSuccess;
    private final Map<U, Integer> numberOfRemainingFailuresForFailure;
    private final Map<Future<T>, Iterable<U>> itemsByReference;
    private boolean failure;

    public QuorumTracker(Iterable<U> allUs, QuorumRequestParameters qrp) {
        numberOfRemainingFailuresForFailure = Maps.newHashMap();
        numberOfRemainingSuccessesForSuccess = Maps.newConcurrentMap();

        for (U item : allUs) {
            numberOfRemainingSuccessesForSuccess.put(item, qrp.getSuccessFactor());
            numberOfRemainingFailuresForFailure.put(item, qrp.getFailureFactor());
        }

        itemsByReference = Maps.newHashMap();
        failure = false;
    }

    static <V, W> QuorumTracker<V, W> of(Iterable<W> allUs, QuorumRequestParameters qrp) {
        return new QuorumTracker<V, W>(allUs, qrp);
    }

    void handleSuccess(Future<T> ref) {
        Preconditions.checkState(failure() == false && success() == false);
        Preconditions.checkArgument(itemsByReference.containsKey(ref));
        for (U cell : itemsByReference.get(ref)) {
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
        itemsByReference.remove(ref);
    }

    void handleFailure(Future<T> ref) {
        Preconditions.checkState(failure() == false && success() == false);
        Preconditions.checkArgument(itemsByReference.containsKey(ref));
        for (U cell : itemsByReference.get(ref)) {
            if (numberOfRemainingFailuresForFailure.containsKey(cell)) {
                int newValue = numberOfRemainingFailuresForFailure.get(cell) - 1;
                if (newValue < 0) {
                    failure = true;
                    break;
                } else {
                    numberOfRemainingFailuresForFailure.put(cell, newValue);
                }
            }
        }
        itemsByReference.remove(ref);
    }

    void registerRef(Future<T> ref, Iterable<U> items) {
        Preconditions.checkState(failure() == false && success() == false);
        itemsByReference.put(ref, items);
    }

    void cancel(boolean mayInterruptIfRunning) {
        for (Future<T> f : itemsByReference.keySet()) {
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
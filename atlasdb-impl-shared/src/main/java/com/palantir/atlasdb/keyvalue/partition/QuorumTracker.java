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

    public static <V, W> QuorumTracker<V, W> of(Iterable<W> allUs, QuorumRequestParameters qrp) {
        return new QuorumTracker<V, W>(allUs, qrp);
    }

    public void handleSuccess(Future<T> ref) {
        Preconditions.checkState(failed() == false && succeeded() == false);
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

    public void handleFailure(Future<T> ref) {
        Preconditions.checkState(failed() == false && succeeded() == false);
        Preconditions.checkArgument(itemsByReference.containsKey(ref));
        for (U cell : itemsByReference.get(ref)) {
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
        itemsByReference.remove(ref);
    }

    public void registerRef(Future<T> ref, Iterable<U> items) {
        Preconditions.checkState(failed() == false && succeeded() == false);
        itemsByReference.put(ref, items);
    }

    public void cancel(boolean mayInterruptIfRunning) {
        for (Future<T> f : itemsByReference.keySet()) {
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
}
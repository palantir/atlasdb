package com.palantir.atlasdb.keyvalue.partition;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;

public class RowQuorumTracker<T> {

    private final Map<byte[], Integer> numberOfRemainingSuccessesForSuccess;
    private final Map<byte[], Integer> numberOfRemainingFailuresForFailure;
    private final Map<T, Set<byte[]>> rowsByReference;
    private boolean failure;
    private final int replicationFactor;
    private final int successFactor;

    /*
     * successFactor - minimum number of successes per cell
     */
    RowQuorumTracker(Iterable<byte[]> allRows, final int replicationFactor, final int successFactor) {
        numberOfRemainingFailuresForFailure = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        numberOfRemainingSuccessesForSuccess = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        rowsByReference = Maps.newHashMap();
        failure = false;
        this.replicationFactor = replicationFactor;
        this.successFactor = successFactor;
        for (byte[] row : allRows) {
            numberOfRemainingSuccessesForSuccess.put(row, successFactor);
            numberOfRemainingFailuresForFailure.put(row, replicationFactor - successFactor);
        }
    }

    public static <V> RowQuorumTracker<V> of(Iterable<byte[]> allRows, final int replicationFactor, final int successFactor) {
        return new RowQuorumTracker<V>(allRows, replicationFactor, successFactor);
    }

    public void handleSuccess(T ref) {
        Preconditions.checkState(failure() == false && success() == false);
        Preconditions.checkState(rowsByReference.containsKey(ref));
        for (byte[] row : rowsByReference.get(ref)) {
            if (numberOfRemainingSuccessesForSuccess.containsKey(row)) {
                int newValue = numberOfRemainingSuccessesForSuccess.get(row) - 1;
                if (newValue == 0) {
                    numberOfRemainingSuccessesForSuccess.remove(row);
                    numberOfRemainingFailuresForFailure.remove(row);
                } else {
                    numberOfRemainingSuccessesForSuccess.put(row, newValue);
                }
            }
        }
    }

    public void handleFailure(T ref) {
        Preconditions.checkState(failure() == false && success() == false);
        Preconditions.checkArgument(rowsByReference.containsKey(ref));
        for (byte[] row : rowsByReference.get(ref)) {
            if (numberOfRemainingFailuresForFailure.containsKey(row)) {
                int newValue = numberOfRemainingFailuresForFailure.get(row) - 1;
                if (newValue == 0) {
                    failure = true;
                    break;
                } else {
                    numberOfRemainingFailuresForFailure.put(row, newValue);
                }
            }
        }
    }

    public void registerRef(T ref, Iterable<byte[]> rows) {
        Preconditions.checkState(failure() == false && success() == false);
        Set<byte[]> set = Sets.newTreeSet(UnsignedBytes.lexicographicalComparator());
        for (byte[] row : rows) {
            set.add(row);
        }
        rowsByReference.put(ref, set);
    }

    public boolean failure() {
        return failure;
    }

    public boolean success() {
        return !failure() && numberOfRemainingSuccessesForSuccess.isEmpty();
    }

    public boolean finished() {
        return failure() || success();
    }
}
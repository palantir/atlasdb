package com.palantir.atlasdb.keyvalue.partition;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;

class RowQuorumTracker<T> {

    private final Map<Cell, Integer> numberOfRemainingSuccessesForSuccess;
    private final Map<Cell, Integer> numberOfRemainingFailuresForFailure;
    private final Map<T, Set<Cell>> cellsByReference;
    private final Set<Cell> cellsSeen;
    private final Set<byte[]> rowsSeen;
    private final Set<byte[]> allRows;
    private boolean failure;
    private final int replicationFactor;
    private final int successFactor;

    /*
     * successFactor - minimum number of successes per cell
     */
    RowQuorumTracker(Set<byte[]> allRows, final int replicationFactor, final int successFactor) {
        numberOfRemainingFailuresForFailure = Maps.newHashMap();
        numberOfRemainingSuccessesForSuccess = Maps.newConcurrentMap();
        cellsByReference = Maps.newHashMap();
        cellsSeen = Sets.newHashSet();
        rowsSeen = Sets.newTreeSet(UnsignedBytes.lexicographicalComparator());
        failure = false;
        this.replicationFactor = replicationFactor;
        this.successFactor = successFactor;
        this.allRows = allRows;
    }

    static <V> RowQuorumTracker<V> of(Iterable<byte[]> allRows, final int replicationFactor, final int successFactor) {
        Set<byte[]> allRowsSet = Sets.newTreeSet(UnsignedBytes.lexicographicalComparator());
        for (byte[] row : allRows) {
            allRowsSet.add(row);
        }
        return new RowQuorumTracker<V>(allRowsSet, replicationFactor, successFactor);
    }

    private void registerIfUnknown(T ref, Cell cell) {
        cellsByReference.get(ref).add(cell);
        if (cellsSeen.contains(cell)) {
            return;
        }
        cellsSeen.add(cell);
        Preconditions.checkArgument(allRows.contains(cell.getRowName()));
        rowsSeen.add(cell.getRowName());
        numberOfRemainingFailuresForFailure.put(cell, replicationFactor - successFactor);
        numberOfRemainingSuccessesForSuccess.put(cell, successFactor);
    }

    void handleSuccess(T ref, Set<Cell> cells) {
        Preconditions.checkState(failure() == false && success() == false);
        for (Cell cell : cellsByReference.get(ref)) {
            registerIfUnknown(ref, cell);
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
        for (Cell cell : cellsByReference.get(ref)) {
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

    void registerRef(T ref) {
        Preconditions.checkState(failure() == false && success() == false);
        cellsByReference.put(ref, Sets.<Cell>newHashSet());
    }

    boolean failure() {
        return failure;
    }

    boolean success() {
        return !failure() && numberOfRemainingSuccessesForSuccess.isEmpty() && rowsSeen.size() == allRows.size();
    }

    boolean finished() {
        return failure() || success();
    }
}
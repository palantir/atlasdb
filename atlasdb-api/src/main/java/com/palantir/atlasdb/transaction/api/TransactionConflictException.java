/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.transaction.api;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import java.io.Serializable;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * Thrown if there is a conflict detected when a transaction is committed.
 * If two concurrent transactions make calls to
 * {@link Transaction#put(TableReference, java.util.Map)} or
 * {@link Transaction#delete(TableReference, Set)} for the same <code>Cell</code>,
 * then this is a write-write conflict.
 * <p>
 * The error message should be detailed about what caused the failure and what other transaction
 * conflicted with this one.
 */
public final class TransactionConflictException extends TransactionFailedRetriableException {
    private static final long serialVersionUID = 1L;
    private final ConcurrentMap<TableReference, ConcurrentNavigableMap<Cell, byte[]>> writesByTable;

    public static class CellConflict implements Serializable {
        private static final long serialVersionUID = 1L;

        private final Cell cell;
        private final String cellString;
        private final long theirStart;
        private final long theirCommit;

        public CellConflict(Cell cell, long theirStart, long theirCommit) {
            this.cell = cell;
            this.cellString = cell.toString();
            this.theirStart = theirStart;
            this.theirCommit = theirCommit;
        }

        public CellConflict(Cell cell, String cellString, long theirStart, long theirCommit) {
            this.cell = cell;
            this.cellString = cellString;
            this.theirStart = theirStart;
            this.theirCommit = theirCommit;
        }

        public final Cell getCell() {
            return cell;
        }

        public final long getTheirStart() {
            return theirStart;
        }

        public static Function<CellConflict, Cell> getCellFunction() {
            return input -> input.cell;
        }

        @Override
        public final String toString() {
            return "CellConflict [cell=" + cellString
                    + ", theirStart=" + theirStart
                    + ", theirCommit=" + theirCommit + "]";
        }
    }

    private final ImmutableList<CellConflict> spanningWrites;
    private final ImmutableList<CellConflict> dominatingWrites;
    private final TableReference conflictingTable;

    /**
     * These conflicts had a start timestamp before our start and a commit timestamp after our start.
     */
    public Collection<CellConflict> getSpanningWrites() {
        return spanningWrites;
    }

    /**
     * These conflicts started and committed after our start timestamp.  Having these kinds of conflicts means that we
     * may be a long running transaction.
     */
    public Collection<CellConflict> getDominatingWrites() {
        return dominatingWrites;
    }

    public TableReference getConflictingTable() {
        return conflictingTable;
    }

    /**
     * DO NOT MERGE 1!11!!!111.
     */
    public ConcurrentMap<TableReference, ConcurrentNavigableMap<Cell, byte[]>> getWritesByTable() {
        return writesByTable;
    }

    public static TransactionConflictException create(
            TableReference tableRef,
            long timestamp,
            ConcurrentMap<TableReference, ConcurrentNavigableMap<Cell, byte[]>> lol,
            Collection<CellConflict> spanningWrites,
            Collection<CellConflict> dominatingWrites,
            long elapsedMillis) {
        StringBuilder sb = new StringBuilder();
        sb.append("Transaction Conflict after ")
                .append(elapsedMillis)
                .append(" ms for table: ")
                .append(tableRef.getQualifiedName())
                .append(" with start timestamp: ")
                .append(timestamp)
                .append('\n');
        if (!spanningWrites.isEmpty()) {
            sb.append("Another transaction wrote values before our start timestamp and committed after. Cells:\n");
            formatConflicts(spanningWrites, sb);
            sb.append('\n');
        }

        if (!dominatingWrites.isEmpty()) {
            sb.append("Another transaction wrote and committed between our start and end ts.")
                    .append(" It is possible we are a long running transaction. Cells:\n");
            formatConflicts(dominatingWrites, sb);
            sb.append('\n');
        }
        return new TransactionConflictException(sb.toString(), lol, spanningWrites, dominatingWrites, tableRef);
    }

    private static void formatConflicts(Collection<CellConflict> conflicts, StringBuilder sb) {
        sb.append("[\n");
        for (CellConflict conflict : conflicts) {
            sb.append(' ');
            sb.append(conflict);
            sb.append(",\n");
        }
        sb.append(']');
    }

    private TransactionConflictException(
            String message,
            ConcurrentMap<TableReference, ConcurrentNavigableMap<Cell, byte[]>> writesByTable,
            Collection<CellConflict> spanningWrites,
            Collection<CellConflict> dominatingWrites,
            TableReference conflictingTable) {
        super(message);
        this.writesByTable = writesByTable;
        this.spanningWrites = ImmutableList.copyOf(spanningWrites);
        this.dominatingWrites = ImmutableList.copyOf(dominatingWrites);
        this.conflictingTable = conflictingTable;
    }
}

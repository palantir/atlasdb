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
package com.palantir.atlasdb.transaction.api;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;

/**
 * Thrown if there is a conflict detected when a transaction is committed.
 * If two concurrent transactions make calls to {@link Transaction#put(com.palantir.atlasdb.keyvalue.api.TableReference, java.util.Map)} or
 * {@link Transaction#delete(com.palantir.atlasdb.keyvalue.api.TableReference, Set)} for the same <code>Cell</code>, then this is a write-write conflict.
 * <p>
 * The error message should be detailed about what caused the failure and what other transaction
 * conflicted with this one.
 */
public class TransactionConflictException extends TransactionFailedRetriableException {
    private static final long serialVersionUID = 1L;

    public static class CellConflict implements Serializable {
        private static final long serialVersionUID = 1L;

        public final Cell cell;
        public final String cellString;
        public final long theirStart;
        public final long theirCommit;

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

        @Override
        public String toString() {
            return "CellConflict [cell=" + cellString + ", theirStart=" + theirStart + ", theirCommit=" +
                theirCommit + "]";
        }

        public static Function<CellConflict, Cell> getCellFunction() {
            return new Function<TransactionConflictException.CellConflict, Cell>() {
                @Override
                public Cell apply(CellConflict input) {
                    return input.cell;
                }
            };
        }
    }

    final ImmutableList<CellConflict> spanningWrites;
    final ImmutableList<CellConflict> dominatingWrites;

    /**
     * These conflicts had a start timestamp before our start and a commit timestamp after our start.
     * @return
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

    public static TransactionConflictException create(TableReference tableRef, long timestamp,
                                                      Collection<CellConflict> spanningWrites, Collection<CellConflict> dominatingWrites, long elapsedMillis) {
        StringBuilder sb = new StringBuilder();
        sb.append("Transaction Conflict after " + elapsedMillis + " ms for table: "
            + tableRef.getQualifiedName() + " with start timestamp: " + timestamp + "\n");
        if (!spanningWrites.isEmpty()) {
            sb.append("Another transaction wrote values before our start timestamp and committed after. Cells:\n");
            formatConflicts(spanningWrites, sb);
            sb.append('\n');
        }

        if (!dominatingWrites.isEmpty()) {
            sb.append("Another transaction wrote and committed between our start and end ts. " +
                "It is possible we are a long running transaction. Cells:\n");
            formatConflicts(dominatingWrites, sb);
            sb.append('\n');
        }
        return new TransactionConflictException(sb.toString(), spanningWrites, dominatingWrites);
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

    private TransactionConflictException(String message,
                                         Collection<CellConflict> spanningWrites,
                                         Collection<CellConflict> dominatingWrites) {
        super(message);
        this.spanningWrites = ImmutableList.copyOf(spanningWrites);
        this.dominatingWrites = ImmutableList.copyOf(dominatingWrites);
    }
}

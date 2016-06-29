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
package com.palantir.atlasdb.keyvalue.remoting.iterators;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.remoting.RemotingKeyValueService;

public class RemoteRowColumnRangeIterator implements RowColumnRangeIterator {
    @JsonProperty("tableRef")
    final TableReference tableRef;
    @JsonProperty("columnRangeSelection")
    final ColumnRangeSelection columnRangeSelection;
    @JsonProperty("timestamp")
    final long timestamp;
    @JsonProperty("hasNext")
    boolean hasNext;
    @JsonProperty("page")
    List<Map.Entry<Cell, Value>> page;
    int position = 0;

    @JsonCreator
    public RemoteRowColumnRangeIterator(@JsonProperty("tableRef") TableReference tableRef,
                                        @JsonProperty("columnRangeSelection") ColumnRangeSelection columnRangeSelection,
                                        @JsonProperty("timestamp") long timestamp,
                                        @JsonProperty("hasNext") boolean hasNext,
                                        @JsonProperty("page") List<Map.Entry<Cell, Value>> page) {
        this.tableRef = tableRef;
        this.columnRangeSelection = columnRangeSelection;
        this.timestamp = timestamp;
        this.hasNext = hasNext;
        if (page == null) {
            this.page = ImmutableList.of();
        } else {
            this.page = page;
        }
        if (this.page.isEmpty() && hasNext) {
            throw new IllegalStateException("Attempting to create a row column page claiming to have more results available while having " +
                    "no results in the current page.");
        }
    }

    @Override
    public Map.Entry<Cell, Value> next() {
        Preconditions.checkState(hasNext());

        if (position < page.size()) {
            return page.get(position++);
        }

        // Download more results from the server
        KeyValueService keyValueService = RemotingKeyValueService.getServiceContext().get();
        if (keyValueService == null) {
            throw new IllegalStateException(
                    "This remote keyvalue service needs to be wrapped with RemotingKeyValueService.createClientSide!");
        }

        Cell lastCell = page.get(page.size() - 1).getKey();
        byte[] row = lastCell.getRowName();
        Preconditions.checkArgument(!RangeRequests.isLastRowName(lastCell.getColumnName()));
        byte[] nextCol = RangeRequests.nextLexicographicName(lastCell.getColumnName());
        RemoteRowColumnRangeIterator result = getMoreRows(keyValueService, tableRef, row, nextCol);
        swapWithNewRows(validateIsRangeIterator(result));

        if (position < page.size()) {
            return page.get(position++);
        } else {
            throw new IllegalStateException();
        }
    }

    protected RemoteRowColumnRangeIterator getMoreRows(KeyValueService kvs, TableReference tableRef, byte[] row, byte[] nextCol) {
        ColumnRangeSelection newColumnRange = new ColumnRangeSelection(nextCol, columnRangeSelection.getEndCol(), columnRangeSelection.getBatchHint());
        Map<byte[], RowColumnRangeIterator> result = kvs.getRowsColumnRange(tableRef, ImmutableList.of(row), newColumnRange, timestamp);
        if (result.isEmpty()) {
            new RemoteRowColumnRangeIterator(tableRef, columnRangeSelection, timestamp, false, ImmutableList.of());
        }
        RowColumnRangeIterator it = Iterables.getOnlyElement(result.values());
        List<Map.Entry<Cell, Value>> page = ImmutableList.copyOf(Iterators.limit(it, columnRangeSelection.getBatchHint()));
        return new RemoteRowColumnRangeIterator(tableRef, columnRangeSelection, timestamp, it.hasNext(), page);
    }

    private void swapWithNewRows(RemoteRowColumnRangeIterator other) {
        hasNext = other.hasNext;
        page = other.page;
        position = 0;
    }

    @Override
    public boolean hasNext() {
        if (position < page.size()) {
            return true;
        }
        return hasNext;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RemoteRowColumnRangeIterator that = (RemoteRowColumnRangeIterator) o;

        if (timestamp != that.timestamp) return false;
        if (hasNext != that.hasNext) return false;
        if (position != that.position) return false;
        if (tableRef != null ? !tableRef.equals(that.tableRef) : that.tableRef != null) return false;
        if (columnRangeSelection != null ? !columnRangeSelection.equals(that.columnRangeSelection) : that.columnRangeSelection != null)
            return false;
        return page != null ? page.equals(that.page) : that.page == null;

    }

    @Override
    public int hashCode() {
        int result = tableRef != null ? tableRef.hashCode() : 0;
        result = 31 * result + (columnRangeSelection != null ? columnRangeSelection.hashCode() : 0);
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (hasNext ? 1 : 0);
        result = 31 * result + (page != null ? page.hashCode() : 0);
        result = 31 * result + position;
        return result;
    }

    static RemoteRowColumnRangeIterator validateIsRangeIterator(RowColumnRangeIterator it) {
        if (!(it instanceof RemoteRowColumnRangeIterator)) {
            throw new IllegalArgumentException("The server-side kvs must be wrapper with RemotingKeyValueService.createServerSide()");
        }
        return (RemoteRowColumnRangeIterator) it;
    }
}

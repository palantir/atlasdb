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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.remoting.RemotingKeyValueService;
import com.palantir.common.base.ClosableIterator;

public abstract class RangeIterator<T> implements ClosableIterator<RowResult<T>> {

    @JsonProperty("tableRef")
    final TableReference tableRef;

    @JsonProperty("range")
    final RangeRequest range;

    @JsonProperty("timestamp")
    final long timestamp;

    @JsonProperty("hasNext")
    boolean hasNext;

    @JsonProperty("page")
    ImmutableList<RowResult<T>> page;

    @JsonProperty("position")
    int position = 0;

    public RangeIterator(TableReference tableRef, RangeRequest range, long timestamp,
                         boolean hasNext, ImmutableList<RowResult<T>> page) {
        this.tableRef = tableRef;
        this.range = range;
        this.timestamp = timestamp;
        this.hasNext = hasNext;
        this.page = page;
    }

    @Override
    public RowResult<T> next() {
        Preconditions.checkState(hasNext());

        if (position < page.size()) {
            return page.get(position++);
        }

        // Download more results from the server
        RowResult<T> lastResult = page.get(page.size()-1);
        byte[] newStart = RangeRequests.getNextStartRow(range.isReverse(), lastResult.getRowName());

        RangeRequest newRange = range.getBuilder().startRowInclusive(newStart).build();
        KeyValueService keyValueService = RemotingKeyValueService.getServiceContext().get();
        if (keyValueService == null) {
            throw new IllegalStateException(
                    "This remote keyvalue service needs to be wrapped with RemotingKeyValueService.createClientSide!");
        }

        ClosableIterator<RowResult<T>> result = getMoreRows(keyValueService, tableRef, newRange, timestamp);
        swapWithNewRows(validateIsRangeIterator(result));

        if (position < page.size()) {
            return page.get(position++);
        } else {
            throw new IllegalStateException();
        }
    }

    protected abstract ClosableIterator<RowResult<T>> getMoreRows(KeyValueService kvs, TableReference tableRef, RangeRequest newRange, long timestamp);

    private void swapWithNewRows(RangeIterator<T> other) {
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
    public void close() {
        // not needed
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !getClass().equals(obj.getClass())) {
            return false;
        }
        RangeIterator<?> other = (RangeIterator<?>) obj;
        if (!Objects.equal(tableRef, other.tableRef)) {
            return false;
        }
        if (timestamp != other.timestamp) {
            return false;
        }
        if (hasNext != other.hasNext) {
            return false;
        }
        if (position != other.position) {
            return false;
        }
        if (!Objects.equal(range, other.range)) {
            return false;
        }
        if (!Objects.equal(page, other.page)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(tableRef, range, page, timestamp, hasNext, position);
    }

    static <T> RangeIterator<T> validateIsRangeIterator(ClosableIterator<RowResult<T>> it) {
        if (!(it instanceof RangeIterator)) {
            throw new IllegalArgumentException("The server-side kvs must be wrapper with RemotingKeyValueService.createServerSide()");
        }
        return (RangeIterator<T>) it;
    }
}

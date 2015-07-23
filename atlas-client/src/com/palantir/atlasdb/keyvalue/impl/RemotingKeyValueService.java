// Copyright 2015 Palantir Technologies
//
// Licensed under the BSD-3 License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://opensource.org/licenses/BSD-3-Clause
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.keyvalue.impl;

import java.io.Serializable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.supplier.ExecutorInheritableServiceContext;
import com.palantir.common.supplier.PopulateServiceContextProxy;
import com.palantir.common.supplier.ServiceContext;

public class RemotingKeyValueService extends ForwardingKeyValueService {
    final static ServiceContext<KeyValueService> remoteServiceContext = ExecutorInheritableServiceContext.create();

    public static KeyValueService createClientSide(final KeyValueService remoteService) {
        return new ForwardingKeyValueService() {
            @Override
            protected KeyValueService delegate() {
                return remoteService;
            }

            @SuppressWarnings("unchecked")
            @Override
            public ClosableIterator<RowResult<Value>> getRange(String tableName,
                                                               RangeRequest rangeRequest,
                                                               long timestamp) {
                ClosableIterator<RowResult<Value>> range = super.getRange(tableName, rangeRequest, timestamp);
                return PopulateServiceContextProxy.newProxyInstanceWithConstantValue(ClosableIterator.class, range, remoteService, remoteServiceContext);
            }
        };
    }

    public static KeyValueService createServerSide(KeyValueService delegate) {
        return new RemotingKeyValueService(delegate);
    }

    final KeyValueService delegate;

    private RemotingKeyValueService(KeyValueService service) {
        this.delegate = service;
    }

    @Override
    protected KeyValueService delegate() {
        return delegate;
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(String tableName,
                                                       RangeRequest range,
                                                       long timestamp) {
        ClosableIterator<RowResult<Value>> it = super.getRange(tableName, range, timestamp);
        try {
            int pageSize = range.getBatchHint() != null ? range.getBatchHint() : 100;
            if (pageSize == 1) {
                pageSize = 2;
            }
            ImmutableList<RowResult<Value>> page = ImmutableList.copyOf(Iterators.limit(it, pageSize));
            if (page.size() < pageSize) {
                return new RangeIterator(tableName, range, timestamp, false, page);
            } else {
                return new RangeIterator(tableName, range, timestamp, true, page.subList(0, pageSize-1));
            }
        } finally {
            it.close();
        }
    }

    static class RangeIterator implements ClosableIterator<RowResult<Value>>, Serializable {
        private static final long serialVersionUID = 1L;

        public RangeIterator(String tableName,
                             RangeRequest range,
                             long timestamp,
                             boolean hasNext,
                             ImmutableList<RowResult<Value>> page) {
            this.tableName = tableName;
            this.range = range;
            this.timestamp = timestamp;
            this.hasNext = hasNext;
            this.page = page;
        }

        final String tableName;
        final RangeRequest range;
        final long timestamp;

        boolean hasNext;
        int position = 0;
        ImmutableList<RowResult<Value>> page;

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public RowResult<Value> next() {
            if (position < page.size()) {
                return page.get(position++);
            }

            RowResult<Value> lastResult = page.get(page.size()-1);
            byte[] newStart = RangeRequests.getNextStartRow(range.isReverse(), lastResult.getRowName());
            RangeRequest newRange = range.getBuilder().startRowInclusive(newStart).build();
            KeyValueService keyValueService = remoteServiceContext.get();
            if (keyValueService == null) {
                throw new IllegalStateException("This remote keyvalue service needs to be wrapped with RemotingKeyValueService.createClientSide");
            }
            ClosableIterator<RowResult<Value>> result = keyValueService.getRange(tableName, newRange, timestamp);
            RangeIterator swap = (RangeIterator) result;
            this.hasNext = swap.hasNext;
            this.page = swap.page;
            this.position = 0;

            if (position < page.size()) {
                return page.get(position++);
            } else {
                throw new IllegalStateException();
            }
        }

        @Override
        public boolean hasNext() {
            if (position < page.size()) {
                return true;
            }
            return hasNext;
        }

        @Override
        public void close() {
            // not needed
        }
    }
}

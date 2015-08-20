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
package com.palantir.atlasdb.keyvalue.remoting;

import java.util.Set;

import javax.annotation.Nullable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.ForwardingKeyValueService;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.supplier.ExecutorInheritableServiceContext;
import com.palantir.common.supplier.PopulateServiceContextProxy;
import com.palantir.common.supplier.ServiceContext;
import com.palantir.util.Pair;

class RemotingKeyValueService extends ForwardingKeyValueService {
    // TODO: Why can this thing be static?
    final static ServiceContext<KeyValueService> serviceContext = ExecutorInheritableServiceContext.create();
    final KeyValueService delegate;

    public static KeyValueService createClientSide(final KeyValueService remoteService) {
        return new ForwardingKeyValueService() {
            @Override
            protected KeyValueService delegate() {
                return remoteService;
            }

            @SuppressWarnings("unchecked") @Override
            public ClosableIterator<RowResult<Value>> getRange(String tableName,
                                                               RangeRequest rangeRequest,
                                                               long timestamp) {
                return PopulateServiceContextProxy.newProxyInstanceWithConstantValue(ClosableIterator.class,
                        RangeIterator.validateIsRangeIterator(super.getRange(tableName, rangeRequest, timestamp)),
                        remoteService, serviceContext);
            }

            @SuppressWarnings("unchecked") @Override
            public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(String tableName,
                                                                               RangeRequest rangeRequest,
                                                                               long timestamp) {
                return PopulateServiceContextProxy.newProxyInstanceWithConstantValue(ClosableIterator.class,
                        RangeIterator.validateIsRangeIterator(super.getRangeWithHistory(tableName, rangeRequest, timestamp)),
                        remoteService, serviceContext);
            }

            @SuppressWarnings("unchecked") @Override
            public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(String tableName,
                                                                               RangeRequest rangeRequest,
                                                                               long timestamp) {
                return PopulateServiceContextProxy.newProxyInstanceWithConstantValue(ClosableIterator.class,
                        RangeIterator.validateIsRangeIterator(super.getRangeOfTimestamps(tableName, rangeRequest, timestamp)),
                        remoteService, serviceContext);
            }
        };
    }

    public static KeyValueService createServerSide(KeyValueService delegate) {
        return new RemotingKeyValueService(delegate);
    }

    private RemotingKeyValueService(KeyValueService service) {
        this.delegate = service;
    }

    @Override
    protected KeyValueService delegate() {
        return delegate;
    }

    @Override
    public RangeIterator<Value> getRange(final String tableName,
                                         final RangeRequest range,
                                         final long timestamp) {
        return transformIterator(tableName, range, timestamp, new Function<Void, ClosableIterator<RowResult<Value>>>() {
            @Override @Nullable
            public ClosableIterator<RowResult<Value>> apply(@Nullable Void input) {
                return RemotingKeyValueService.super.getRange(tableName, range, timestamp);
            }
        }, new Function<Pair<Boolean, ImmutableList<RowResult<Value>>>, RangeIterator<Value>>() {
            @Override @Nullable
            public RangeIterator<Value> apply(@Nullable Pair<Boolean, ImmutableList<RowResult<Value>>> input) {
                return new ValueRangeIterator(tableName, range, timestamp, input.lhSide, input.rhSide);
            }
        });
    }

    @Override
    public RangeIterator<Set<Value>> getRangeWithHistory(final String tableName,
                                                         final RangeRequest rangeRequest,
                                                         final long timestamp) {
        return transformIterator(tableName, rangeRequest, timestamp, new Function<Void, ClosableIterator<RowResult<Set<Value>>>>() {
            @Override @Nullable
            public ClosableIterator<RowResult<Set<Value>>> apply(@Nullable Void input) {
                return RemotingKeyValueService.super.getRangeWithHistory(tableName, rangeRequest, timestamp);
            }
        }, new Function<Pair<Boolean, ImmutableList<RowResult<Set<Value>>>>, RangeIterator<Set<Value>>>(){
            @Override @Nullable
            public RangeIterator<Set<Value>> apply(@Nullable Pair<Boolean, ImmutableList<RowResult<Set<Value>>>> input) {
                return new HistoryRangeIterator(tableName, rangeRequest, timestamp, input.lhSide, input.rhSide);
            }

        });
    }

    @Override
    public RangeIterator<Set<Long>> getRangeOfTimestamps(final String tableName,
                                                         final RangeRequest rangeRequest,
                                                         final long timestamp) {
        return transformIterator(tableName, rangeRequest, timestamp, new Function<Void, ClosableIterator<RowResult<Set<Long>>>>() {
            @Override @Nullable
            public ClosableIterator<RowResult<Set<Long>>> apply(@Nullable Void input) {
                return RemotingKeyValueService.super.getRangeOfTimestamps(tableName, rangeRequest, timestamp);
            }
        }, new Function<Pair<Boolean, ImmutableList<RowResult<Set<Long>>>>, RangeIterator<Set<Long>>>() {
            @Override @Nullable
            public RangeIterator<Set<Long>> apply(@Nullable Pair<Boolean, ImmutableList<RowResult<Set<Long>>>> input) {
                return new TimestampsRangeIterator(tableName, rangeRequest, timestamp, input.lhSide, input.rhSide);
            }
        });
    }

    private static final SimpleModule kvsModule = new SimpleModule(); static {
        kvsModule.addKeyDeserializer(Cell.class, CellAsKeyDeserializer.instance());
        kvsModule.addKeyDeserializer(byte[].class, BytesAsKeyDeserializer.instance());
        kvsModule.addKeySerializer(Cell.class, SaneAsKeySerializer.instance());
        kvsModule.addKeySerializer(byte[].class, SaneAsKeySerializer.instance());
        kvsModule.addSerializer(RowResult.class, RowResultSerializer.instance());
        kvsModule.addDeserializer(RowResult.class, RowResultDeserializer.instance());
    }
    private static final ObjectMapper kvsMapper = new ObjectMapper(); static {
        kvsMapper.registerModule(kvsModule);
        kvsMapper.registerModule(new GuavaModule());
    }
    public static SimpleModule kvsModule() {
        return kvsModule;
    }
    public static ObjectMapper kvsMapper() {
        return kvsMapper;
    }

    private static <T> RangeIterator<T> transformIterator(String tableName, RangeRequest range,
                                                   long timestamp, Function<Void, ClosableIterator<RowResult<T>>> itSupplier,
                                                   Function<Pair<Boolean, ImmutableList<RowResult<T>>>, RangeIterator<T>> resultSupplier) {
        ClosableIterator<RowResult<T>> it = itSupplier.apply(null);
        try {
            int pageSize = range.getBatchHint() != null ? range.getBatchHint() : 100;
            if (pageSize == 1) {
                pageSize = 2;
            }
            ImmutableList<RowResult<T>> page = ImmutableList.copyOf(Iterators.limit(it, pageSize));
            if (page.size() < pageSize) {
                return resultSupplier.apply(Pair.create(false, page));
            } else {
                return resultSupplier.apply(Pair.create(true, page.subList(0, pageSize - 1)));
            }
        } finally {
            it.close();
        }
    }
}

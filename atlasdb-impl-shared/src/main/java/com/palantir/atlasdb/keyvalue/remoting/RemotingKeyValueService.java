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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.ForwardingKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.map.DynamicPartitionMapImpl;
import com.palantir.atlasdb.keyvalue.remoting.iterators.HistoryRangeIterator;
import com.palantir.atlasdb.keyvalue.remoting.iterators.RangeIterator;
import com.palantir.atlasdb.keyvalue.remoting.iterators.RemoteRowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.remoting.iterators.TimestampsRangeIterator;
import com.palantir.atlasdb.keyvalue.remoting.iterators.ValueRangeIterator;
import com.palantir.atlasdb.keyvalue.remoting.outofband.OutboxShippingInterceptor;
import com.palantir.atlasdb.keyvalue.remoting.proxy.VersionCheckProxy;
import com.palantir.atlasdb.keyvalue.remoting.serialization.BytesAsKeyDeserializer;
import com.palantir.atlasdb.keyvalue.remoting.serialization.CellAsKeyDeserializer;
import com.palantir.atlasdb.keyvalue.remoting.serialization.RowResultDeserializer;
import com.palantir.atlasdb.keyvalue.remoting.serialization.RowResultSerializer;
import com.palantir.atlasdb.keyvalue.remoting.serialization.SaneAsKeySerializer;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.supplier.ExecutorInheritableServiceContext;
import com.palantir.common.supplier.PopulateServiceContextProxy;
import com.palantir.common.supplier.RemoteContextHolder;
import com.palantir.common.supplier.RemoteContextHolder.RemoteContextType;
import com.palantir.common.supplier.ServiceContext;
import com.palantir.util.Pair;

import feign.Feign;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;

public final class RemotingKeyValueService extends ForwardingKeyValueService {
    private static final Logger log = LoggerFactory.getLogger(RemotingKeyValueService.class);
    private static final ServiceContext<KeyValueService> serviceContext = ExecutorInheritableServiceContext.create();
    private static final SimpleModule kvsModule = new SimpleModule();
    private static final ObjectMapper kvsMapper = new ObjectMapper();

    private final KeyValueService delegate;

    static {
        kvsModule.addKeyDeserializer(Cell.class, CellAsKeyDeserializer.instance());
        kvsModule.addKeyDeserializer(byte[].class, BytesAsKeyDeserializer.instance());
        kvsModule.addKeySerializer(Cell.class, SaneAsKeySerializer.instance());
        kvsModule.addKeySerializer(byte[].class, SaneAsKeySerializer.instance());
        kvsModule.addSerializer(RowResult.class, RowResultSerializer.instance());
        kvsModule.addDeserializer(RowResult.class, RowResultDeserializer.instance());
        kvsModule.addSerializer(DynamicPartitionMapImpl.class, DynamicPartitionMapImpl.Serializer.instance());
        kvsModule.addDeserializer(DynamicPartitionMapImpl.class, DynamicPartitionMapImpl.Deserializer.instance());
        kvsModule.addAbstractTypeMapping(RowColumnRangeIterator.class, RemoteRowColumnRangeIterator.class);

        kvsMapper.registerModule(kvsModule);
        kvsMapper.registerModule(new GuavaModule());
    }


    public static ServiceContext<KeyValueService> getServiceContext() {
        return serviceContext;
    }

    public static SimpleModule kvsModule() {
        return kvsModule;
    }

    public static ObjectMapper kvsMapper() {
        return kvsMapper;
    }

    /**
     * This is to inject the local KVS instance reference into the context.
     * It is used by the range iterators to download additional pages of data.
     */
    private static KeyValueService createClientSideInternal(final KeyValueService remoteService) {
        return new ForwardingKeyValueService() {
            @Override
            protected KeyValueService delegate() {
                return remoteService;
            }

            @SuppressWarnings("unchecked")
            private <T extends ClosableIterator<?>> T withKvs(T it) {
                return (T) PopulateServiceContextProxy.newProxyInstanceWithConstantValue(
                        ClosableIterator.class, it, delegate(), serviceContext);
            }

            @Override
            public ClosableIterator<RowResult<Value>> getRange(TableReference tableRef,
                                                               RangeRequest rangeRequest,
                                                               long timestamp) {
                return withKvs(super.getRange(tableRef, rangeRequest, timestamp));
            }

            @Override
            public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(TableReference tableRef,
                                                                               RangeRequest rangeRequest,
                                                                               long timestamp) {
                return withKvs(super.getRangeWithHistory(tableRef, rangeRequest, timestamp));
            }

            @Override
            public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(TableReference tableRef,
                                                                               RangeRequest rangeRequest,
                                                                               long timestamp) {
                return withKvs(super.getRangeOfTimestamps(tableRef, rangeRequest, timestamp));
            }
        };
    }

    public enum LongContextType implements RemoteContextType<Long> {
        PM_VERSION {
            @Override
            public Class<Long> getValueType() {
                return Long.class;
            }
        }
    }

    public enum StringContextType implements RemoteContextType<String> {
        PMS_URI {
            @Override
            public Class<String> getValueType() {
                return String.class;
            }
        }
    }

    /**
     * This will convert a remote KVS service URI into a fully functional Java class instance
     * that supports empty byte arrays, exceptions and sends the partition map version out-of-band
     * automatically.
     *
     * @param localVersionSupplier The version of local partition map to be sent out-of-band.
     */
    public static KeyValueService createClientSide(String uri, Supplier<Long> localVersionSupplier) {
        ServiceContext<Long> outboxVersionCtx = RemoteContextHolder.OUTBOX
                .getProviderForKey(LongContextType.PM_VERSION);

        KeyValueService remotingKvs = Feign.builder()
                .encoder(new OctetStreamDelegateEncoder(new JacksonEncoder(kvsMapper())))
                .decoder(new OctetStreamDelegateDecoder(new JacksonDecoder(kvsMapper())))
                .errorDecoder(KeyValueServiceErrorDecoder.instance())
                .contract(new JAXRSContract())
                .requestInterceptor(new OutboxShippingInterceptor(kvsMapper()))
                .target(KeyValueService.class, uri);

        KeyValueService versionSettingRemotingKvs = PopulateServiceContextProxy.newProxyInstance(
                KeyValueService.class, remotingKvs, localVersionSupplier, outboxVersionCtx);
        KeyValueService pagingIteratorsVersionSettingRemotingKvs = createClientSideInternal(versionSettingRemotingKvs);

        return pagingIteratorsVersionSettingRemotingKvs;
    }

    /**
     * This will convert the range iterators to serializable page-based versions and will
     * ensure that the partition map version of the client and of the server are compatible.
     * @param serverVersionSupplier Use <code>Suppliers.&lt;Long&gt;ofInstance(-1L)</code>
     *                              if you want to disable version check.
     */
    public static KeyValueService createServerSide(KeyValueService delegate, Supplier<Long> serverVersionSupplier) {
        final KeyValueService versionCheckingKvs = VersionCheckProxy.newProxyInstance(delegate, serverVersionSupplier);
        return new RemotingKeyValueService(versionCheckingKvs);
    }

    private RemotingKeyValueService(KeyValueService service) {
        this.delegate = service;
    }

    @Override
    protected KeyValueService delegate() {
        return delegate;
    }

    @Override
    public RangeIterator<Value> getRange(final TableReference tableRef,
                                         final RangeRequest range,
                                         final long timestamp) {
        return transformIterator(
                range,
                super.getRange(tableRef, range, timestamp),
                input -> new ValueRangeIterator(tableRef, range, timestamp, input.lhSide, input.rhSide));
    }

    @Override
    public RangeIterator<Set<Value>> getRangeWithHistory(final TableReference tableRef,
                                                         final RangeRequest rangeRequest,
                                                         final long timestamp) {
        return transformIterator(
                rangeRequest,
                super.getRangeWithHistory(tableRef, rangeRequest, timestamp),
                input -> new HistoryRangeIterator(tableRef, rangeRequest, timestamp, input.lhSide, input.rhSide));
    }

    @Override
    public RangeIterator<Set<Long>> getRangeOfTimestamps(final TableReference tableRef,
                                                         final RangeRequest rangeRequest,
                                                         final long timestamp) {
        return transformIterator(
                rangeRequest,
                super.getRangeOfTimestamps(tableRef, rangeRequest, timestamp),
                input -> new TimestampsRangeIterator(tableRef, rangeRequest, timestamp, input.lhSide, input.rhSide));
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection,
            long timestamp) {
        Map<byte[], RowColumnRangeIterator> rowsColumnRange =
                super.getRowsColumnRange(tableRef, rows, columnRangeSelection, timestamp);
        Map<byte[], RowColumnRangeIterator> transformed = Maps.transformValues(rowsColumnRange, it -> {
            List<Map.Entry<Cell, Value>> page =
                    ImmutableList.copyOf(Iterators.limit(it, columnRangeSelection.getBatchHint()));
            return new RemoteRowColumnRangeIterator(
                    tableRef,
                    columnRangeSelection,
                    timestamp,
                    it.hasNext(),
                    page);
        });
        return transformed;
    }

    // This method transforms an iterator into paging iterator that can be
    // sent over-the-wire in json.
    private static <T> RangeIterator<T> transformIterator(
            RangeRequest range,
            ClosableIterator<RowResult<T>> closableIterator,
            Function<Pair<Boolean, ImmutableList<RowResult<T>>>, RangeIterator<T>> resultSupplier) {
        try {
            int pageSize = range.getBatchHint() != null ? range.getBatchHint() : 100;
            if (pageSize == 1) {
                pageSize = 2;
            }
            ImmutableList<RowResult<T>> page = ImmutableList.copyOf(Iterators.limit(closableIterator, pageSize));
            if (page.size() < pageSize) {
                return resultSupplier.apply(Pair.create(false, page));
            } else {
                return resultSupplier.apply(Pair.create(true, page.subList(0, pageSize - 1)));
            }
        } finally {
            closableIterator.close();
        }
    }
}

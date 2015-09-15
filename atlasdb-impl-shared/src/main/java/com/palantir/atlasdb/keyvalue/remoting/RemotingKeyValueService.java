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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.ForwardingKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.atlasdb.keyvalue.partition.map.DynamicPartitionMapImpl;
import com.palantir.atlasdb.keyvalue.remoting.iterators.HistoryRangeIterator;
import com.palantir.atlasdb.keyvalue.remoting.iterators.RangeIterator;
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

public class RemotingKeyValueService extends ForwardingKeyValueService {
    private static final Logger log = LoggerFactory.getLogger(RemotingKeyValueService.class);
    private final static ServiceContext<KeyValueService> serviceContext = ExecutorInheritableServiceContext.create();

    private final KeyValueService delegate;

    public static ServiceContext<KeyValueService> getServiceContext() {
        return serviceContext;
    }


    /**
     * This is to inject the local KVS instance reference into the context.
     * It is used by the range iterators to download additional pages of data.
     *
     * @param remoteService
     * @return
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
            public ClosableIterator<RowResult<Value>> getRange(String tableName,
                                                               RangeRequest rangeRequest,
                                                               long timestamp) {
                return withKvs(super.getRange(tableName, rangeRequest, timestamp));
            }

            @Override
            public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(String tableName,
                                                                               RangeRequest rangeRequest,
                                                                               long timestamp) {
                return withKvs(super.getRangeWithHistory(tableName, rangeRequest, timestamp));
            }

            @Override
            public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(String tableName,
                                                                               RangeRequest rangeRequest,
                                                                               long timestamp) {
                return withKvs(super.getRangeOfTimestamps(tableName, rangeRequest, timestamp));
            }
        };
    }

    public enum LONG_HOLDER implements RemoteContextType<Long> {
        PM_VERSION {
            @Override
            public Class<Long> getValueType() {
                return Long.class;
            }
        }
    }

    public enum STRING_HOLDER implements RemoteContextType<String> {
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
     * @param uri
     * @param localVersionSupplier The version of local partition map to be sent out-of-band.
     * @return
     */
    public static KeyValueService createClientSide(String uri, Supplier<Long> localVersionSupplier) {
        ServiceContext<Long> outboxVersionCtx = RemoteContextHolder.OUTBOX.getProviderForKey(LONG_HOLDER.PM_VERSION);
        ServiceContext<String> outboxPmsUriCtx = RemoteContextHolder.OUTBOX.getProviderForKey(STRING_HOLDER.PMS_URI);

        KeyValueService remotingKvs = Feign.builder()
                .encoder(new OctetStreamDelegateEncoder(new JacksonEncoder(kvsMapper())))
                .decoder(new OctetStreamDelegateDecoder(new JacksonDecoder(kvsMapper())))
                .errorDecoder(KeyValueServiceErrorDecoder.instance())
                .contract(new JAXRSContract())
                .requestInterceptor(new OutboxShippingInterceptor(kvsMapper()))
                .target(KeyValueService.class, uri);

        KeyValueService versionSettingRemotingKvs = PopulateServiceContextProxy.newProxyInstance(
                KeyValueService.class, remotingKvs, localVersionSupplier, outboxVersionCtx);
        KeyValueService pmsUriSettingVersionSettingRemotingKvs = PopulateServiceContextProxy.newProxyInstance(
                KeyValueService.class, versionSettingRemotingKvs, Suppliers.ofInstance("xxx"), outboxPmsUriCtx);

        KeyValueService pagingIteratorsPmsUriSettingVersionSettingRemotingKvs = createClientSideInternal(pmsUriSettingVersionSettingRemotingKvs);

        return pagingIteratorsPmsUriSettingVersionSettingRemotingKvs;
    }

    /**
     * This will convert the range iterators to serializable page-based versions and will
     * ensure that the partition map version of the client and of the server are compatible.
     * @param delegate
     * @param serverVersionSupplier Use <code>Suppliers.<Long>ofInstance(-1L)</code> if you want to disable version check.
     * @return
     */
    public static KeyValueService createServerSide(KeyValueService delegate, Supplier<Long> serverVersionSupplier, Function<? super DynamicPartitionMap, Void> serverPartitionMapUpdater) {
        final KeyValueService kvs = new RemotingKeyValueService(delegate);
        return VersionCheckProxy.newProxyInstance(kvs, serverVersionSupplier, serverPartitionMapUpdater);
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
        return transformIterator(tableName, range, timestamp, super.getRange(tableName, range, timestamp),
            new Function<Pair<Boolean, ImmutableList<RowResult<Value>>>, RangeIterator<Value>>() {
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
        return transformIterator(tableName, rangeRequest, timestamp, super.getRangeWithHistory(tableName, rangeRequest, timestamp),
            new Function<Pair<Boolean, ImmutableList<RowResult<Set<Value>>>>, RangeIterator<Set<Value>>>(){
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
        return transformIterator(tableName, rangeRequest, timestamp, super.getRangeOfTimestamps(tableName, rangeRequest, timestamp),
            new Function<Pair<Boolean, ImmutableList<RowResult<Set<Long>>>>, RangeIterator<Set<Long>>>() {
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
        kvsModule.addSerializer(DynamicPartitionMapImpl.class, DynamicPartitionMapImpl.Serializer.instance());
        kvsModule.addDeserializer(DynamicPartitionMapImpl.class, DynamicPartitionMapImpl.Deserializer.instance());
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
                                                   long timestamp, ClosableIterator<RowResult<T>> closableIterator,
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

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
package com.palantir.atlasdb.keyvalue.remoting.proxy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Set;

import com.google.common.reflect.AbstractInvocationHandler;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.ForwardingKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.exception.ClientVersionTooOldException;
import com.palantir.atlasdb.keyvalue.partition.exception.EndpointVersionTooOldException;
import com.palantir.common.base.ClosableIterator;

/**
 * This is to inject remote partition map service URI to a VersionTooOldException. New partition map
 * can be downloaded from that service.
 *
 * It is meant to be used by <code>KeyValueEndpoint</code>, because it has both KeyValueService reference
 * and the URI of the corresponding PartitionMapService.
 *
 * @see KeyValueEndpoint
 * @see KeyValueService
 *
 * @author htarasiuk
 *
 */
public class FillInUrlProxy<T> extends AbstractInvocationHandler {

    final T delegate;
    final String pmsUri;

    private FillInUrlProxy(T delegate, String pmsUri) {
        this.delegate = delegate;
        this.pmsUri = pmsUri;
    }

    /**
     * This proxy will ensure that all KVS method calls will fill in the pmsUri in
     * case of an exception. Futhermore the iterators returned by this KVS will have
     * all their methods fill in the pmsUri as well.
     *
     * @param delegate key value service delegate
     * @param pmsUri partition map service URI
     * @return proxied key value service
     */
    public static KeyValueService newFillInUrlProxy(KeyValueService delegate, String pmsUri) {
        // First make the kvs iterators fill in the pmsUri
        KeyValueService kvsWithUrlFillingIterators = new KeyValueServiceWithUrlFillingIterators(delegate, pmsUri);
        // Finally make the kvs method calls fill in the pmsUri
        FillInUrlProxy<KeyValueService> kvsHandler = new FillInUrlProxy<>(kvsWithUrlFillingIterators, pmsUri);
        return (KeyValueService) Proxy.newProxyInstance(KeyValueService.class.getClassLoader(),
                new Class<?>[] { KeyValueService.class }, kvsHandler);
    }

    @SuppressWarnings("unchecked")
    public static <T> ClosableIterator<RowResult<T>> newFillInUrlProxy(ClosableIterator<RowResult<T>> delegate, String pmsUri) {
        FillInUrlProxy<ClosableIterator<RowResult<T>>> itHandler = new FillInUrlProxy<>(delegate, pmsUri);
        return (ClosableIterator<RowResult<T>>) Proxy.newProxyInstance(
                ClosableIterator.class.getClassLoader(), new Class<?>[] { ClosableIterator.class }, itHandler);
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(delegate, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof ClientVersionTooOldException) {
            	throw new ClientVersionTooOldException(pmsUri);
            }
            if (cause instanceof EndpointVersionTooOldException) {
                throw new EndpointVersionTooOldException(pmsUri);
            }
            throw cause;
        }
    }

    /**
     * This guy wraps the iterator returned by delegate to include the pmsUri
     * in case their methods should throw a VersionMismatch.
     *
     * @author htarasiuk
     *
     */
    public static class KeyValueServiceWithUrlFillingIterators extends ForwardingKeyValueService {

        private final KeyValueService delegate;
        private final String pmsUri;

        public KeyValueServiceWithUrlFillingIterators(KeyValueService delegate, String pmsUri) {
            this.delegate = delegate;
            this.pmsUri = pmsUri;
        }

        @Override
        protected KeyValueService delegate() {
            return delegate;
        }

        @Override
        public ClosableIterator<RowResult<Value>> getRange(TableReference tableRef,
                                                           RangeRequest rangeRequest, long timestamp) {
            return newFillInUrlProxy(super.getRange(tableRef, rangeRequest, timestamp), pmsUri);
        }

        @Override
        public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(
                TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
            return newFillInUrlProxy(super.getRangeWithHistory(tableRef, rangeRequest, timestamp), pmsUri);
        }

        @Override
        public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(
                TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
            return newFillInUrlProxy(super.getRangeOfTimestamps(tableRef, rangeRequest, timestamp), pmsUri);
        }

    }

}

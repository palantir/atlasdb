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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.ForwardingKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.exception.ClientVersionTooOldException;
import com.palantir.atlasdb.keyvalue.partition.exception.EndpointVersionTooOldException;
import com.palantir.atlasdb.keyvalue.remoting.RemotingKeyValueService.LONG_HOLDER;
import com.palantir.atlasdb.keyvalue.remoting.RemotingKeyValueService.STRING_HOLDER;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.supplier.RemoteContextHolder;
import com.palantir.common.supplier.ServiceContext;

/**
 * This is for the endpoint to ensure that client and server partition map versions are compatible.
 * It will throw <code>VersionTooOldException</code> if the client is out of date.
 *
 * The server version supplier is passed as an argument. Client version supplier is taken
 * from <code>RemoteContextHolder.INBOX.getProviderForKey(HOLDER.PM_VERSION)</code>.
 *
 * @see ClientVersionTooOldException
 * @see RemoteContextHolder
 * @see LONG_HOLDER
 * @see STRING_HOLDER
 *
 * @author htarasiuk
 *
 */
public class VersionCheckProxy<T> implements InvocationHandler {
    private static final Logger log = LoggerFactory.getLogger(VersionCheckProxy.class);
    private final Supplier<Long> serverVersionProvider;
    private final Supplier<Long> clientVersionProvider;
    private final T delegate;

    private VersionCheckProxy(Supplier<Long> serverVersionProvider, Supplier<Long> clientVersionProvider, T delegate) {
        this.serverVersionProvider = serverVersionProvider;
        this.clientVersionProvider = clientVersionProvider;
        this.delegate = delegate;
    }

    private static final boolean isMethodVersionExempt(Method method) {
        return method.getDeclaringClass() != KeyValueService.class
                && method.getDeclaringClass() != ClosableIterator.class
                && method.getDeclaringClass() != Iterator.class;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        // Only check the version for appropriate methods
        if (!isMethodVersionExempt(method)) {
            Long clientVersion = clientVersionProvider.get();
            Long serverVersion = Preconditions.checkNotNull(serverVersionProvider.get());
            if (serverVersion < 0L) {
                // In this case the version check is simply disabled.
                assert clientVersion == null || clientVersion < 0;
            } else {
                if (!serverVersion.equals(clientVersion)) {
                    log.warn("Server map version: " + serverVersion + ", client map version: " + clientVersion);
                }
                if (clientVersion < serverVersion) {
                    throw new ClientVersionTooOldException();
                }
                if (clientVersion > serverVersion) {
                    throw new EndpointVersionTooOldException();
                }
            }
        }

        try {
            return method.invoke(delegate, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    /**
     * This proxy checks the client version based on the out-of-band data that came with this request.
     *
     * @param delegate
     * @param serverVersionProvider Use <code>Suppliers.<Long>ofInstance(-1L)</code> to disable version check. In
     * such case this proxy is just a no-op.
     * @return
     */
    public static KeyValueService newProxyInstance(KeyValueService delegate, Supplier<Long> serverVersionProvider) {
        ServiceContext<Long> remoteVersionClientCtx = RemoteContextHolder.INBOX.getProviderForKey(LONG_HOLDER.PM_VERSION);
        KeyValueService kvsWithVersionCheckIterators = new KeyValueServiceWithVersionCheckingIterators(delegate, serverVersionProvider);
        VersionCheckProxy<KeyValueService> vcp = new VersionCheckProxy<>(serverVersionProvider, remoteVersionClientCtx, kvsWithVersionCheckIterators);
        return (KeyValueService) Proxy.newProxyInstance(
                KeyValueService.class.getClassLoader(), new Class<?>[] { KeyValueService.class }, vcp);
    }

    /**
     * Caveat: If client version is changed after the iterator is created, it will not throw.
     * But if we say that endpoint should auto-update on request only, this should be fine.
     * (The endpoint version will be updated on next kvs request.)
     * Note: This is relevant even when the kvs is remoted and iterators are serialized - in case
     * partition map version changes during the serialization (after obtaining the iterator but
     * before downloading entire page).
     *
     * @param delegate
     * @param serverVersionProvider
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T> ClosableIterator<RowResult<T>> newProxyInstance(ClosableIterator<RowResult<T>> delegate, Supplier<Long> serverVersionProvider) {
        // In case of the iterators we have to ensure that our endpoint version did not change since the iterator was created.
        // Whether endpoint version matches client version has been checked during iterator creation.
        // Therefore the "clientVersion" here is the server version at this moment.
        VersionCheckProxy<ClosableIterator<RowResult<T>>> vcp = new VersionCheckProxy<>(
                serverVersionProvider, Suppliers.ofInstance(serverVersionProvider.get()), delegate);
        return (ClosableIterator<RowResult<T>>) Proxy.newProxyInstance(
                ClosableIterator.class.getClassLoader(), new Class<?>[] { ClosableIterator.class }, vcp);
    }

    public static class KeyValueServiceWithVersionCheckingIterators extends ForwardingKeyValueService {

        private final KeyValueService delegate;
        private final Supplier<Long> serverVersionSupplier;

        public KeyValueServiceWithVersionCheckingIterators(
                KeyValueService delegate, Supplier<Long> serverVersionSupplier) {
            this.delegate = delegate;
            this.serverVersionSupplier = serverVersionSupplier;
        }

        @Override
        protected KeyValueService delegate() {
            return delegate;
        }

        @Override
        public ClosableIterator<RowResult<Value>> getRange(String tableName,
                RangeRequest rangeRequest, long timestamp) {
            return newProxyInstance(super.getRange(tableName, rangeRequest, timestamp), serverVersionSupplier);
        }

        @Override
        public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(
                String tableName, RangeRequest rangeRequest, long timestamp) {
            return newProxyInstance(super.getRangeWithHistory(tableName, rangeRequest, timestamp), serverVersionSupplier);
        }

        @Override
        public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(
                String tableName, RangeRequest rangeRequest, long timestamp) {
            return newProxyInstance(super.getRangeOfTimestamps(tableName, rangeRequest, timestamp), serverVersionSupplier);
        }

    }

}
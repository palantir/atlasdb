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
import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.reflect.AbstractInvocationHandler;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
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
public class VersionCheckProxy<T> extends AbstractInvocationHandler {
    private static final Logger log = LoggerFactory.getLogger(VersionCheckProxy.class);
    private final Supplier<Long> serverVersionProvider;
    private final Supplier<Long> clientVersionProvider;
    private final T delegate;

    private VersionCheckProxy(Supplier<Long> serverVersionProvider, Supplier<Long> clientVersionProvider, T delegate) {
        this.serverVersionProvider = serverVersionProvider;
        this.clientVersionProvider = clientVersionProvider;
        this.delegate = delegate;
    }

    private static boolean isMethodVersionExempt(Method method) {
        return method.getDeclaringClass() != KeyValueService.class
                && method.getDeclaringClass() != ClosableIterator.class
                && method.getDeclaringClass() != Iterator.class;
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
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
     * @param delegate key value service delegate
     * @param serverVersionProvider Use <code>Suppliers.<Long>ofInstance(-1L)</code> to disable version check. In
     * such case this proxy is just a no-op.
     * @return proxied key value service
     */
    public static KeyValueService newProxyInstance(KeyValueService delegate, Supplier<Long> serverVersionProvider) {
        ServiceContext<Long> remoteVersionClientCtx = RemoteContextHolder.INBOX.getProviderForKey(LONG_HOLDER.PM_VERSION);
        KeyValueService kvsWithVersionCheckIterators = new KeyValueServiceWithVersionCheckingIterators(delegate, serverVersionProvider);
        VersionCheckProxy<KeyValueService> vcp = new VersionCheckProxy<>(serverVersionProvider, remoteVersionClientCtx, kvsWithVersionCheckIterators);
        return (KeyValueService) Proxy.newProxyInstance(
                KeyValueService.class.getClassLoader(), new Class<?>[] { KeyValueService.class }, vcp);
    }

    /**
     * This will simply throw EndpointVersionTooOldException whenever the
     * invoke-time version is different (must be greater) than the creation-time
     * version.
     *
     * @param delegate closeable iterator delegate
     * @param currentVersionProvider current version provider
     * @return proxied closeable iterator
     */
    @SuppressWarnings("unchecked")
    public static <T> ClosableIterator<RowResult<T>> invalidateOnVersionChangeProxy(ClosableIterator<RowResult<T>> delegate, Supplier<Long> currentVersionProvider) {
        VersionCheckProxy<ClosableIterator<RowResult<T>>> vcp = new VersionCheckProxy<>(
                currentVersionProvider, Suppliers.ofInstance(currentVersionProvider.get()), delegate);
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
        public ClosableIterator<RowResult<Value>> getRange(TableReference tableRef,
                                                           RangeRequest rangeRequest, long timestamp) {
            return invalidateOnVersionChangeProxy(super.getRange(tableRef, rangeRequest, timestamp), serverVersionSupplier);
        }

        @Override
        public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(
                TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
            return invalidateOnVersionChangeProxy(super.getRangeWithHistory(tableRef, rangeRequest, timestamp), serverVersionSupplier);
        }

        @Override
        public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(
                TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
            return invalidateOnVersionChangeProxy(super.getRangeOfTimestamps(tableRef, rangeRequest, timestamp), serverVersionSupplier);
        }

    }

}

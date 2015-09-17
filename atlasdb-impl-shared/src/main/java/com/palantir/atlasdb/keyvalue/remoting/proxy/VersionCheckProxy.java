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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.atlasdb.keyvalue.partition.exception.ClientVersionTooOldException;
import com.palantir.atlasdb.keyvalue.partition.exception.EndpointVersionTooOldException;
import com.palantir.atlasdb.keyvalue.remoting.RemotingKeyValueService.LONG_HOLDER;
import com.palantir.atlasdb.keyvalue.remoting.RemotingKeyValueService.STRING_HOLDER;
import com.palantir.atlasdb.keyvalue.remoting.RemotingPartitionMapService;
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
public class VersionCheckProxy implements InvocationHandler {
    private static final Logger log = LoggerFactory.getLogger(VersionCheckProxy.class);
    private final Supplier<Long> serverVersionProvider;
    private final Function<? super DynamicPartitionMap, Void> serverPartitionMapUpdater;
    private final KeyValueService delegate;

    private VersionCheckProxy(Supplier<Long> serverVersionProvider, Function<? super DynamicPartitionMap, Void> serverPartitionMapUpdater, KeyValueService delegate) {
        this.serverVersionProvider = serverVersionProvider;
        this.serverPartitionMapUpdater = serverPartitionMapUpdater;
        this.delegate = delegate;
    }


    /*
     * Table metadata methods are version check-exempt because the metadata
     * is stored on all enpoints anyway.
     *
     * This is necessary because the metadata skips transaction manager and
     * does not retry automatically on retryable exception.
     *
     * Some other "technical" methods are also exempt.
     */
    private static final boolean isMethodVersionExempt(Method method) {
        return method.getDeclaringClass() != KeyValueService.class;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        ServiceContext<Long> remoteVersionClientCtx = RemoteContextHolder.INBOX.getProviderForKey(LONG_HOLDER.PM_VERSION);
        ServiceContext<String> newPmsUriCtx = RemoteContextHolder.INBOX.getProviderForKey(STRING_HOLDER.PMS_URI);

        // Only check the version for appropriate methods
        if (!isMethodVersionExempt(method)) {
            Long clientVersion = remoteVersionClientCtx.get();
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
                    String newPmsUri = newPmsUriCtx.get();
                    if (newPmsUri != null) {
                        try {
                            serverPartitionMapUpdater.apply(RemotingPartitionMapService.createClientSide(newPmsUri).getMap());
                        } catch (RuntimeException e) {
                            log.warn("Could not update server partition map");
                            throw new EndpointVersionTooOldException("Server partition map out-of-date and update failed.", e);
                        }
                    } else {
                        throw new EndpointVersionTooOldException("Server partition map out-of-date.");
                    }
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
     *
     * @param delegate
     * @param serverVersionProvider Use <code>Suppliers.<Long>ofInstance(-1L)</code> to disable version check. In
     * such case this proxy is just a no-op.
     * @return
     */
    public static KeyValueService newProxyInstance(KeyValueService delegate, Supplier<Long> serverVersionProvider,
                                                   Function<? super DynamicPartitionMap, Void> serverPartitionMapUpdater) {
        VersionCheckProxy vcp = new VersionCheckProxy(serverVersionProvider, serverPartitionMapUpdater, delegate);
        return (KeyValueService) Proxy.newProxyInstance(
                KeyValueService.class.getClassLoader(),
                new Class<?>[] { KeyValueService.class }, vcp);
    }

}
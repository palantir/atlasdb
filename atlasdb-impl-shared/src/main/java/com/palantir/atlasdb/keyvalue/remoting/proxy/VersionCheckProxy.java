package com.palantir.atlasdb.keyvalue.remoting.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.partition.exception.VersionTooOldException;
import com.palantir.atlasdb.keyvalue.remoting.RemotingKeyValueService.HOLDER;
import com.palantir.common.supplier.RemoteContextHolder;
import com.palantir.common.supplier.ServiceContext;

/**
 * This is for the endpoint to ensure that client and server partition map versions are compatible.
 * It will throw <code>VersionTooOldException</code> if the client is out of date.
 *
 * The server version supplier is passed as an argument. Client version supplier is taken
 * from <code>RemoteContextHolder.INBOX.getProviderForKey(HOLDER.PM_VERSION)</code>.
 *
 * @see VersionTooOldException
 * @see RemoteContextHolder
 * @see HOLDER
 *
 * @author htarasiuk
 *
 */
public class VersionCheckProxy implements InvocationHandler {
    private static final Logger log = LoggerFactory.getLogger(VersionCheckProxy.class);
    private final Supplier<Long> serverVersionProvider;
    private final KeyValueService delegate;

    private VersionCheckProxy(Supplier<Long> serverVersionProvider, KeyValueService delegate) {
        this.serverVersionProvider = serverVersionProvider;
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
    private static final boolean isMethodVersionExempts(Method method) {
        return method.getDeclaringClass() == KeyValueService.class && EXEMPT_METHODS.contains(method.getName());
    }

    private static final Set<String> EXEMPT_METHODS = ImmutableSet.<String>builder()
            // Table and table metadata methods
            .add("dropTable")
            .add("createTable")
            .add("createTables")
            .add("truncateTable")
            .add("truncateTables")
            .add("getAllTableNames")
            .add("getAllTableNames")
            .add("getMetadataForTable")
            .add("getMetadataForTables")
            .add("putMetadataForTable")
            .add("putMetadataForTables")
            // Technical methods
            .add("close")
            .add("teardown")
            .add("initializeFromFreshInstance")
            .add("compactInternally")
            .build();

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        ServiceContext<Long> remoteClientCtx = RemoteContextHolder.INBOX.getProviderForKey(HOLDER.PM_VERSION);

        // Only check the version for the interface methods.
        if (method.getDeclaringClass() == KeyValueService.class && !isMethodVersionExempts(method)) {
            Long clientVersion = remoteClientCtx.get();
            Long serverVersion = Preconditions.checkNotNull(serverVersionProvider.get());
            if (serverVersion < 0L) {
                // In this case the version check is simply disabled.
                assert clientVersion == null || clientVersion < 0;
            } else {
                if (clientVersion < serverVersion) {
                    throw new VersionTooOldException();
                }
                if (clientVersion > serverVersion) {
                    // TODO:
                    log.warn("Server partition map version is out-of-date.");
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
    public static KeyValueService newProxyInstance(KeyValueService delegate, Supplier<Long> serverVersionProvider) {
        VersionCheckProxy vcp = new VersionCheckProxy(serverVersionProvider, delegate);
        return (KeyValueService) Proxy.newProxyInstance(
                KeyValueService.class.getClassLoader(),
                new Class<?>[] { KeyValueService.class }, vcp);
    }

}
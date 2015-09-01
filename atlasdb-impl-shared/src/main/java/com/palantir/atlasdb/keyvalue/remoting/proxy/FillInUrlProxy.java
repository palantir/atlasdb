package com.palantir.atlasdb.keyvalue.remoting.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.exception.VersionTooOldException;

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
public class FillInUrlProxy implements InvocationHandler {

    final KeyValueService remoteKvs;
    final String pmsUri;

    private FillInUrlProxy(KeyValueService delegate,
                           String pmsUri) {
        this.remoteKvs = delegate;
        this.pmsUri = pmsUri;
    }

    public static KeyValueService newFillInUrlProxy(KeyValueService delegate, String pmsUri) {
        FillInUrlProxy handler = new FillInUrlProxy(delegate, pmsUri);
        return (KeyValueService) Proxy.newProxyInstance( KeyValueService.class.getClassLoader(),
                new Class<?>[] { KeyValueService.class }, handler);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(remoteKvs, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof VersionTooOldException) {
            	throw new VersionTooOldException(pmsUri);
            }
            throw cause;
        }
    }

}
package com.palantir.leader.proxy;

import java.util.concurrent.atomic.AtomicBoolean;

import com.palantir.common.proxy.SimulatingServerProxy;

public final class SimulatingFailingServerProxy {

    public static <T> T newProxyInstance(Class<T> interfaceClass, T delegate, long sleep, AtomicBoolean throwException) {
        return SimulatingServerProxy.newProxyInstance(
                interfaceClass,
                ToggleableExceptionProxy.newProxyInstance(interfaceClass, delegate, throwException, new RuntimeException()),
                sleep);
    }

    private SimulatingFailingServerProxy() {
        throw new AssertionError("uninstantiable");
    }
}

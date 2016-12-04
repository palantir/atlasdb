/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.timelock.copycat;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import javax.ws.rs.ServiceUnavailableException;

import org.immutables.value.Value;

import com.google.common.reflect.AbstractInvocationHandler;

import io.atomix.copycat.server.CopycatServer;

public class CopycatInvalidatingLeaderProxy<T> extends AbstractInvocationHandler {
    private final CopycatServer copycatServer;
    private final AtomicReference<TermWrapped<T>> delegateRef;
    private final Supplier<T> delegateSupplier;

    private CopycatInvalidatingLeaderProxy(CopycatServer copycatServer,
            Supplier<T> delegateSupplier) {
        this.copycatServer = copycatServer;
        this.delegateRef = new AtomicReference<>();
        this.delegateSupplier = delegateSupplier;
    }

    public static <T> T create(
            CopycatServer server,
            Supplier<T> delegateSupplier,
            Class<T> interfaceClass) {
        CopycatInvalidatingLeaderProxy<T> proxy = new CopycatInvalidatingLeaderProxy<>(
                server,
                delegateSupplier);

        return (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] { interfaceClass, Closeable.class },
                proxy);
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        long currentTerm = copycatServer.cluster().term();
        if (isLeader()) {
            TermWrapped<T> delegate = delegateRef.get();
            while (delegate == null || delegate.term() < currentTerm) {
                TermWrapped<T> newDelegate = ImmutableTermWrapped.of(currentTerm, delegateSupplier.get());
                if (delegateRef.compareAndSet(delegate, newDelegate)) {
                    closeIfNecessary(delegate);
                } else {
                    closeIfNecessary(newDelegate);
                }
                delegate = delegateRef.get();
            }

            if (delegate.term() == currentTerm) {
                return method.invoke(delegate.delegate(), args);
            }
        }
        clearDelegate();
        throw new ServiceUnavailableException(
                String.format("This node (%s) is not the leader (%s)",
                        copycatServer.cluster().member().id(),
                        copycatServer.cluster().leader() == null ? "none at the moment" :
                                copycatServer.cluster().leader().id()),
                0L);
    }

    private boolean isLeader() {
        return copycatServer.state() == CopycatServer.State.LEADER;
    }

    private void clearDelegate() throws IOException {
        TermWrapped<T> wrappedDelegate = delegateRef.getAndSet(null);
        closeIfNecessary(wrappedDelegate);
    }

    private void closeIfNecessary(TermWrapped<T> wrappedDelegate) throws IOException {
        if (wrappedDelegate != null && wrappedDelegate.delegate() instanceof Closeable) {
            ((Closeable) wrappedDelegate.delegate()).close();
        }
    }


    @Value.Immutable
    interface TermWrapped<U> {
        @Value.Parameter
        long term();

        @Value.Parameter
        U delegate();
    }
}

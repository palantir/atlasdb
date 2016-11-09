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
package com.palantir.atlasdb.timelock.atomix;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import javax.ws.rs.ServiceUnavailableException;

import com.google.common.reflect.AbstractInvocationHandler;
import com.google.common.util.concurrent.Futures;

import io.atomix.group.LocalMember;
import io.atomix.variables.DistributedValue;

public final class InvalidatingLeaderProxy<T> extends AbstractInvocationHandler {
    private final LocalMember localMember;
    private final DistributedValue<String> leaderId;
    private final AtomicReference<T> delegateRef = new AtomicReference<>();
    private final Supplier<T> delegateSupplier;

    private InvalidatingLeaderProxy(
            LocalMember localMember,
            DistributedValue<String> leaderId,
            Supplier<T> delegateSupplier) {
        this.localMember = localMember;
        this.leaderId = leaderId;
        this.delegateSupplier = delegateSupplier;
    }

    public static <T> T create(
            LocalMember localMember,
            DistributedValue<String> leaderId,
            Supplier<T> delegateSupplier,
            Class<T> interfaceClass) {
        InvalidatingLeaderProxy<T> proxy = new InvalidatingLeaderProxy<>(
                localMember,
                leaderId,
                delegateSupplier);

        return (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] { interfaceClass, Closeable.class },
                proxy);
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        if (Objects.equals(localMember.id(), Futures.getUnchecked(leaderId.get()))) {
            Object delegate = delegateRef.get();
            while (delegate == null) {
                delegateRef.compareAndSet(null, delegateSupplier.get());
                delegate = delegateRef.get();
            }
            return method.invoke(delegate, args);
        } else {
            clearDelegate();
            throw new ServiceUnavailableException("This node is not the leader", 0L);
        }
    }

    private void clearDelegate() throws IOException {
        Object delegate = delegateRef.getAndSet(null);
        if (delegate != null && delegate instanceof Closeable) {
            ((Closeable) delegate).close();
        }
    }
}

/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.paxos;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.reflect.AbstractInvocationHandler;
import com.palantir.common.remoting.ServiceNotAvailableException;

public class DelegatingServiceProxy<T> extends AbstractInvocationHandler {
    private final AtomicReference<T> delegateReference;

    DelegatingServiceProxy(AtomicReference<T> delegateReference) {
        this.delegateReference = delegateReference;
    }

    public static <T> T newProxyInstance(
            AtomicReference<T> decoratedService, Class<T> clazz) {
        DelegatingServiceProxy<T> service =
                new DelegatingServiceProxy<>(decoratedService);
        return (T) Proxy.newProxyInstance(
                clazz.getClassLoader(),
                new Class[] { clazz },
                service);
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        T delegate = delegateReference.get();
        if (delegate == null) {
            throw new ServiceNotAvailableException("This service is not ready yet.");
        }
        return method.invoke(delegate, args);
    }
}

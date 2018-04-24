/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.common.proxy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import com.google.common.base.Preconditions;
import com.palantir.util.jmx.OperationTimer;
import com.palantir.util.jmx.OperationTimer.TimingState;

public class TimingProxy implements DelegatingInvocationHandler {

    @SuppressWarnings("unchecked")
    public static <T> T newProxyInstance(Class<T> interfaceClass, T delegate, OperationTimer timer) {
        return (T)Proxy.newProxyInstance(interfaceClass.getClassLoader(),
            new Class<?>[] {interfaceClass}, new TimingProxy(delegate, timer));
    }

    final private Object delegate;
    final private OperationTimer timer;

    private TimingProxy(Object delegate, OperationTimer timer) {
        Preconditions.checkNotNull(delegate);
        Preconditions.checkNotNull(timer);
        this.delegate = delegate;
        this.timer = timer;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        TimingState token = timer.begin(method.getName());
        assert token != null;
        try {
            return method.invoke(delegate, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        } finally {
            token.end();
        }
    }

    @Override
    public Object getDelegate() {
        return delegate;
    }

}

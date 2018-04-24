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

import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import com.google.common.base.Preconditions;
import com.palantir.util.ObjectInputStreamFactory;

public class SerializingProxy implements DelegatingInvocationHandler {

    public static <T> T newProxyInstance(Class<T> interfaceClass, T delegate) {
        return newProxyInstance(interfaceClass, delegate, (is, codebase) -> new ObjectInputStream(is));
    }

    @SuppressWarnings("unchecked")
    public static <T> T newProxyInstance(Class<T> interfaceClass, T delegate, ObjectInputStreamFactory factory) {
        return (T)Proxy.newProxyInstance(interfaceClass.getClassLoader(),
            new Class<?>[] {interfaceClass}, new SerializingProxy(delegate, factory));
    }

    final Object delegate;
    final ObjectInputStreamFactory factory;

    private SerializingProxy(Object delegate, ObjectInputStreamFactory factory) {
        Preconditions.checkNotNull(factory);
        Preconditions.checkNotNull(delegate);
        this.delegate = delegate;
        this.factory = factory;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        final Object[] argsCopy;
        if (args == null) {
            argsCopy = null;
        } else {
            argsCopy = new Object[args.length];
            for (int i = 0 ; i < args.length ; i++) {
                argsCopy[i] = SerializingUtils.copy(args[i], factory);
                if (argsCopy[i] == null && args[i] != null) {
                    throw new NotSerializableException("failed to serialize object"
                        + " in method " + method
                        + " with arg number " + i
                        + " with class " + args[i].getClass()
                        + " with value " + args[i]);
                }
            }
        }

        try {
            return SerializingUtils.copy(method.invoke(delegate, argsCopy), factory);
        } catch (InvocationTargetException e) {
            throw SerializingUtils.copy(e.getCause());
        }
    }

    @Override
    public Object getDelegate() {
        return delegate;
    }

}

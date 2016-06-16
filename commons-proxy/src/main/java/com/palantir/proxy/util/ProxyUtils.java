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
package com.palantir.proxy.util;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import com.palantir.proxy.exception.ProxyException;

public class ProxyUtils {
    public static Object invokeAndUnwrapITEs(Object object, Method method, Object[] args) throws Throwable {
        Object returnValue;
        try {
            returnValue = method.invoke(object, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
        return returnValue;
    }

    public static boolean isHashCode(Method m) throws ProxyException {
        try {
            return m.equals(Object.class.getMethod("hashCode", new Class[]{}));
        } catch (NoSuchMethodException nsme) {
            throw new ProxyException(nsme);
        }
    }

    public static boolean isEquals(Method m) throws ProxyException {
        try {
            return m.equals(Object.class.getMethod("equals", Object.class));
        } catch (NoSuchMethodException nsme) {
            throw new ProxyException(nsme);
        }
    }

    public static boolean isToString(Method m) throws ProxyException {
        try {
            return m.equals(Object.class.getMethod("toString", new Class[]{}));
        } catch (NoSuchMethodException nsme) {
            throw new ProxyException(nsme);
        }
    }

    public static boolean isProxiableMethod(Method method) throws ProxyException {
        return !isHashCode(method) && !isEquals(method) && !isToString(method);
    }

    public static Object invokeUnproxiableMethod(Method method, Object proxyObject, Object[] args) throws ProxyException {
        if (ProxyUtils.isEquals(method)) {
            return proxyObject == args[0];
        } else if (ProxyUtils.isHashCode(method)) {
            return System.identityHashCode(proxyObject);
        } else if (ProxyUtils.isToString(method)) {
            return proxyObject.getClass().getName() + "@" + Integer.toHexString(proxyObject.hashCode());
        } else {
            throw new IllegalArgumentException("Method should be not be invoked directly on proxyObject: " + method.toString());
        }
    }

    /**
     * @param iface main interface to proxy
     * @param delegate delegate class whose interfaces ot proxy
     * @param handler proxy invocation handler
     * @return a new proxy instance that implements the specified interface as well as all the
     *         interfaces from the delegate class
     */
    public static <T> T newProxy(Class<T> iface, Class<?> delegate, InvocationHandler handler) {
        checkIsInterface(iface);
        return iface.cast(Proxy.newProxyInstance(
                iface.getClassLoader(),
                ProxyUtils.interfaces(iface, delegate),
                handler));
    }

    /**
     * @return the set of interfaces for the specified classes
     * @throws IllegalArgumentException if the specified iface is not an interface
     */
    public static Class<?>[] interfaces(Class<?> iface,
                                        Collection<Class<?>> additionalInterfaces,
                                        Class<?> delegateClass) {
        checkIsInterface(iface);
        Set<Class<?>> interfaces = new LinkedHashSet<Class<?>>();
        interfaces.add(iface);
        interfaces.addAll(additionalInterfaces);
        if (delegateClass.isInterface()) {
            interfaces.add(delegateClass);
        }
        interfaces.addAll(Arrays.asList(delegateClass.getInterfaces()));

        checkAreAllInterfaces(interfaces);
        return interfaces.toArray(new Class<?>[interfaces.size()]);
    }

    /**
     * @return the set of interfaces for the specified classes
     * @throws IllegalArgumentException if the specified iface is not an interface
     */
    public static Class<?>[] interfaces(Class<?> iface, Class<?> delegateClass) {
        return interfaces(iface, Collections.<Class<?>>emptySet(), delegateClass);
    }

    private static void checkIsInterface(Class<?> iface) {
        if (!iface.isInterface()) {
            throw new IllegalArgumentException(iface + " is not an interface");
        }
    }

    private static void checkAreAllInterfaces(Set<Class<?>> interfaces) {
        for (Class<?> possibleInterface : interfaces) {
            checkIsInterface(possibleInterface);
        }
    }

}

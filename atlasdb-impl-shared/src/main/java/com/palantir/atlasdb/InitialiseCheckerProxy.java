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

package com.palantir.atlasdb;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyFactory;

public final class InitialiseCheckerProxy {
    private static Logger log = LoggerFactory.getLogger(InitializingObject.class);

    private InitialiseCheckerProxy() {
        //factory
    }

    public static  <T> T newProxyInstance(Class<?>[] classes,
            Object[] objects,
            Class<T> clazz)
            throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        ProxyFactory factory = new ProxyFactory();
        factory.setSuperclass(clazz);
        factory.setFilter(m -> {
            // ignore initialize() or isInitialized()
            return !Arrays.stream(InitializingObject.class.getMethods())
                    .map(Method::getName).collect(Collectors.toSet()).contains(m.getName());
        });

        Object toReturn = factory.create(classes, objects);
        Supplier<Boolean> isInitialized = ((InitializingObject) toReturn)::isInitialized;
        MethodHandler handler = (self, method, proceed, args) -> {
            if (isInitialized.get()) {
                return proceed.invoke(self, args);
            }
            log.error("The object of type {} is not initialised yet. Hence cannot call method {}.", self.getClass(), method.getName());
            throw new IllegalStateException(String.format("The class %s is not initialised yet. Hence cannot call method %s.", self.getClass(), method.getName()));
        };
        ((Proxy) toReturn).setHandler(handler);
        return (T) toReturn;
    }
}


/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.timelock.paxos;

import com.google.common.reflect.AbstractInvocationHandler;
import com.palantir.leader.NotCurrentLeaderException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicReference;

public final class LeadershipInitializationProxy<T> extends AbstractInvocationHandler {
    private static final RuntimeException EXCEPTION = new NotCurrentLeaderException(
            "This node has not fully " + "initialized yet, and is not ready to be the leader.");

    private final AtomicReference<T> initializationReference;

    public LeadershipInitializationProxy(AtomicReference<T> initializationReference) {
        this.initializationReference = initializationReference;
    }

    public static <T> T newProxyInstance(AtomicReference<T> reference, Class<T> clazz) {
        LeadershipInitializationProxy<T> service = new LeadershipInitializationProxy<>(reference);
        return (T) Proxy.newProxyInstance(clazz.getClassLoader(), new Class[] {clazz}, service);
    }

    @Override
    protected Object handleInvocation(Object _proxy, Method method, Object[] args) throws Throwable {
        T target = initializationReference.get();
        if (target == null) {
            throw EXCEPTION;
        }
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}

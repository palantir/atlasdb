/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.leader.proxy;

import java.io.Closeable;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.function.Supplier;

import com.google.common.reflect.AbstractInvocationHandler;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.TimeLockCorruptionException;
import com.palantir.leader.health.TimeLockCorruptionHealthCheck;

public class TimeLockCorruptionDetectingProxy<T> extends AbstractInvocationHandler {
    private final AwaitingLeadershipProxy<T> delegate;
    private final TimeLockCorruptionHealthCheck corruptionDetectionHealthCheck;


    public TimeLockCorruptionDetectingProxy(AwaitingLeadershipProxy<T> delegate,
            TimeLockCorruptionHealthCheck corruptionDetectionHealthCheck) {
        this.delegate = delegate;
        this.corruptionDetectionHealthCheck = corruptionDetectionHealthCheck;
    }

    public static <U> U newProxyInstance(Class<U> interfaceClass,
            Supplier<U> delegateSupplier,
            LeaderElectionService leaderElectionService,
            TimeLockCorruptionHealthCheck corruptionDetectionHealthCheck) {
        TimeLockCorruptionDetectingProxy<U> proxy = new TimeLockCorruptionDetectingProxy(
                new AwaitingLeadershipProxy(delegateSupplier, leaderElectionService, interfaceClass),
                corruptionDetectionHealthCheck);
        return (U) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] { interfaceClass, Closeable.class },
                proxy);
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        if (corruptionDetectionHealthCheck.isHealthy()) {
            return delegate.handleInvocation(proxy, method, args);
        } else {
            throw new TimeLockCorruptionException("There are sign of corruption in TimeLock");
        }
    }
}

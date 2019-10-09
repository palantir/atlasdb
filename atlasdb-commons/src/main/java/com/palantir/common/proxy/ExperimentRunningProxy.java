/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.common.proxy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.AbstractInvocationHandler;
import com.palantir.exception.NotInitializedException;
import com.palantir.logsafe.SafeArg;

public final class ExperimentRunningProxy<T> extends AbstractInvocationHandler {
    private static final Logger log = LoggerFactory.getLogger(ExperimentRunningProxy.class);
    private static final Duration REFRESH_INTERVAL = Duration.ofMinutes(10);

    private final T experimentalService;
    private final T fallbackService;
    private final BooleanSupplier useExperimental;
    private final Clock clock;

    private final AtomicReference<Instant> nextPermittedExperiment = new AtomicReference<>(Instant.MIN);

    @VisibleForTesting
    ExperimentRunningProxy(
            T experimentalService,
            T fallbackService,
            BooleanSupplier useExperimental,
            Clock clock) {
        this.experimentalService = experimentalService;
        this.fallbackService = fallbackService;
        this.useExperimental = useExperimental;
        this.clock = clock;
    }

    public static <T> T newProxyInstance(
            T experimentalService, T fallbackService, BooleanSupplier useExperimental, Class<T> clazz) {
        ExperimentRunningProxy<T> service =
                new ExperimentRunningProxy<>(experimentalService, fallbackService, useExperimental, Clock.systemUTC());
        return (T) Proxy.newProxyInstance(
                clazz.getClassLoader(),
                new Class[] { clazz },
                service);
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        boolean runExperiment = useExperimental();
        Object target = runExperiment ? experimentalService : fallbackService;
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            if (runExperiment) {
                markExperimentFailure(e);
            }
            if (e.getTargetException() instanceof NotInitializedException) {
                log.warn("Resource is not initialized yet!");
            }
            throw e.getTargetException();
        }
    }

    private boolean useExperimental() {
        return useExperimental.getAsBoolean() && Instant.now(clock).compareTo(nextPermittedExperiment.get()) > 0;
    }

    private void markExperimentFailure(Exception exception) {
        nextPermittedExperiment.accumulateAndGet(Instant.now(clock).plus(REFRESH_INTERVAL),
                (existing, current) -> existing.compareTo(current) > 0 ? existing : current);
        log.info("Experiment failed; we will revert to the fallback service. We will allow attempting to use"
                + " the experimental service again in {}.", SafeArg.of("retryDuration", REFRESH_INTERVAL), exception);
    }
}

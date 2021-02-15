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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.reflect.AbstractInvocationHandler;
import com.palantir.exception.NotInitializedException;
import com.palantir.logsafe.SafeArg;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ExperimentRunningProxy<T> extends AbstractInvocationHandler {
    private static final Logger log = LoggerFactory.getLogger(ExperimentRunningProxy.class);
    static final Duration REFRESH_INTERVAL = Duration.ofMinutes(10);
    static final Duration CLIENT_REFRESH_INTERVAL = Duration.ofMinutes(30);

    private final Supplier<T> refreshingExperimentalServiceSupplier;
    private final T fallbackService;
    private final BooleanSupplier useExperimental;
    private final BooleanSupplier enableFallback;
    private final Clock clock;
    private final Runnable errorTask;

    private final AtomicReference<Instant> nextPermittedExperiment = new AtomicReference<>(Instant.MIN);

    @VisibleForTesting
    ExperimentRunningProxy(
            Supplier<T> experimentalServiceSupplier,
            T fallbackService,
            BooleanSupplier useExperimental,
            BooleanSupplier enableFallback,
            Clock clock,
            Runnable errorTask) {
        this.refreshingExperimentalServiceSupplier = Suppliers.memoizeWithExpiration(
                experimentalServiceSupplier::get, CLIENT_REFRESH_INTERVAL.toMinutes(), TimeUnit.MINUTES);
        this.fallbackService = fallbackService;
        this.useExperimental = useExperimental;
        this.enableFallback = enableFallback;
        this.clock = clock;
        this.errorTask = errorTask;
    }

    @SuppressWarnings("unchecked")
    public static <T> T newProxyInstance(
            Supplier<T> experimentalServiceSupplier,
            T fallbackService,
            BooleanSupplier useExperimental,
            BooleanSupplier enableFallback,
            Class<T> clazz,
            Runnable markErrorMetric) {
        ExperimentRunningProxy<T> service = new ExperimentRunningProxy<>(
                experimentalServiceSupplier,
                fallbackService,
                useExperimental,
                enableFallback,
                Clock.systemUTC(),
                markErrorMetric);
        return (T) Proxy.newProxyInstance(clazz.getClassLoader(), new Class[] {clazz}, service);
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        boolean runExperiment = useExperimental();
        Object target = runExperiment ? refreshingExperimentalServiceSupplier.get() : fallbackService;
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            if (runExperiment) {
                markExperimentFailure(e);
            }
            if (e.getCause() instanceof NotInitializedException) {
                log.warn("Resource is not initialized yet!");
            }
            throw e.getCause();
        }
    }

    private boolean useExperimental() {
        if (!useExperimental.getAsBoolean()) {
            return false;
        }

        if (!enableFallback.getAsBoolean()) {
            return true;
        }

        return sufficientTimeSinceFailure();
    }

    private boolean sufficientTimeSinceFailure() {
        return Instant.now(clock).compareTo(nextPermittedExperiment.get()) > 0;
    }

    private void markExperimentFailure(Exception exception) {
        if (enableFallback.getAsBoolean()) {
            nextPermittedExperiment.accumulateAndGet(
                    Instant.now(clock).plus(REFRESH_INTERVAL),
                    (existing, current) -> existing.compareTo(current) > 0 ? existing : current);
            log.info(
                    "Experiment failed; we will revert to the fallback service. We will allow attempting to use the "
                            + "experimental service again after a timeout.",
                    SafeArg.of("timeout", REFRESH_INTERVAL),
                    exception);
        }
        errorTask.run();
    }
}

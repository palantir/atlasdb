/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.common.concurrent;

import com.palantir.tritium.metrics.MetricRegistries;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link ThreadFactory} that lets you specify threads with a default name
 * and an auto-generated sequence number.
 *
 * @author regs
 */
public class NamedThreadFactory implements ThreadFactory {
    private final String prefix;
    private final boolean isDaemon;
    private final AtomicLong count = new AtomicLong();
    private final ThreadFactory threadFactory;

    /**
     * Creates a new thread factory that will construct non-daemon threads with names like
     * <i>prefix</i>-<i>seqence_number</i>, where sequence number counts up from zero,
     * and prefix is as specified in the constructor.
     *
     * @param prefix The prefix for each constructed thread.
     */
    public NamedThreadFactory(String prefix) {
        this(prefix, true);
    }

    /**
     * Creates a new thread factory that will construct threads with names like
     * <i>prefix</i>-<i>seqence_number</i>, where sequence number counts up from zero,
     * and prefix is as specified in the constructor.
     *
     * @param prefix The prefix for each constructed thread.
     * @param isDaemon {@code true} iff the constructed threads should be daemon threads.
     */
    @SuppressWarnings("deprecation") // No reasonable way to pass a TaggedMetricRegistry
    public NamedThreadFactory(String prefix, boolean isDaemon) {
        this.prefix = prefix;
        this.isDaemon = isDaemon;
        this.threadFactory = MetricRegistries.instrument(
                SharedTaggedMetricRegistries.getSingleton(), Executors.defaultThreadFactory(), prefix);
    }

    /** {@inheritDoc} */
    @Override
    public Thread newThread(Runnable runnable) {
        Thread thread = threadFactory.newThread(runnable);
        thread.setName(prefix + "-" + count.getAndIncrement());
        thread.setDaemon(isDaemon);
        thread.setUncaughtExceptionHandler(AtlasUncaughtExceptionHandler.INSTANCE);
        return thread;
    }

    String getPrefix() {
        return prefix;
    }
}

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

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.palantir.logsafe.SafeArg;
import com.palantir.tritium.metrics.MetricRegistries;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;

/**
 * A {@link ThreadFactory} that lets you specify threads with a default name
 * and an auto-generated sequence number.
 *
 * @author regs
 */
public class NamedThreadFactory implements ThreadFactory {
    private static final Logger log = LoggerFactory.getLogger(NamedThreadFactory.class);

    private final String prefix;
    private final boolean isDaemon;
    private final AtomicLong count = new AtomicLong();
    private final ThreadFactory threadFactory;
    private final Timer threadCreate;
    private final Counter threadCreateFailed;

    /**
     * Creates a new thread factory that will construct non-daemon threads with names like
     * <i>prefix</i>-<i>seqence_number</i>, where sequence number counts up from zero,
     * and prefix is as specified in the constructor.
     *
     * @param prefix The prefix for each constructed thread.
     */
    public NamedThreadFactory(String prefix) {
        this(prefix, false);
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
                SharedTaggedMetricRegistries.getSingleton(),
                Executors.defaultThreadFactory(),
                prefix);
        threadCreate = SharedTaggedMetricRegistries.getSingleton().timer(MetricName.builder()
                .safeName("atlas.executor.threads.created")
                .putSafeTags("executor", prefix)
                .build());
        threadCreateFailed = SharedTaggedMetricRegistries.getSingleton().counter(MetricName.builder()
                .safeName("atlas.executor.threads.createFailure")
                .putSafeTags("executor", prefix)
                .build());
    }

    /** {@inheritDoc} */
    @Override
    public Thread newThread(Runnable runnable) {
        Timer.Context time = threadCreate.time();
        try {
            Thread thread = threadFactory.newThread(runnable);
            thread.setName(prefix + "-" + count.getAndIncrement());
            thread.setDaemon(isDaemon);
            return thread;
        } catch (Throwable e) {
            log.error("Failed to create thread", SafeArg.of("prefix", prefix), e);
            threadCreateFailed.inc();
            throw e;
        } finally {
            time.stop();
        }
    }

    String getPrefix() {
        return prefix;
    }
}

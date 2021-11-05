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

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.Runnables;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tracing.Tracers;
import com.palantir.tritium.metrics.MetricRegistries;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.jboss.threads.ViewExecutor;

/**
 * Please always use the static methods in this class instead of the ones in {@link
 * Executors}, because the executors returned by these methods will propagate
 * {@link ExecutorInheritableThreadLocal} variables.
 *
 * @author jtamer
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public final class PTExecutors {
    private static final SafeLogger log = SafeLoggerFactory.get(PTExecutors.class);

    private static final Supplier<ExecutorService> SHARED_EXECUTOR = Suppliers.memoize(() ->
            // Shared pool uses 60 second idle thread timeouts for greater reuse
            Executors.newCachedThreadPool(new NamedThreadFactory("ptexecutors-shared", true)));

    private static final String FILE_NAME_FOR_THIS_CLASS = PTExecutors.class.getSimpleName() + ".java";

    /**
     * Default keep-alive time on thread pool executors handed out by this class. This timeout is
     * applied to core pool threads as well.
     * <p>
     * Note that this particular timeout is set to be quite low in order to be conservative. We've
     * run into a number of issues (e.g., QA-44927) where thread pool mismanagement has lead to
     * OutOfMemoryExceptions.
     */
    private static final int DEFAULT_THREAD_POOL_TIMEOUT_MILLIS = 5000;

    private static final RejectedExecutionHandler defaultHandler = new AbortPolicy();

    /**
     * Creates a thread pool that creates new threads as needed, but will reuse previously
     * constructed threads when they are available. These pools will typically improve the
     * performance of programs that execute many short-lived asynchronous tasks. Calls to
     * <tt>execute</tt> will reuse previously constructed threads if available. If no existing
     * thread is available, a new thread will be created and added to the pool. Threads that have
     * not been used for sixty seconds are terminated and removed from the cache. Thus, a pool that
     * remains idle for long enough will not consume any resources. Note that pools with similar
     * properties but different details (for example, timeout parameters) may be created using
     * {@link ThreadPoolExecutor} constructors.
     *
     * @return the newly created thread pool
     */
    public static ExecutorService newCachedThreadPool() {
        return newCachedThreadPool(computeBaseThreadName());
    }

    /**
     * Creates a thread pool that creates new threads as needed, but will reuse previously
     * constructed threads when they are available. These pools will typically improve the
     * performance of programs that execute many short-lived asynchronous tasks. Calls to
     * <tt>execute</tt> will reuse previously constructed threads if available. If no existing
     * thread is available, a new thread will be created and added to the pool. Threads that have
     * not been used for sixty seconds are terminated and removed from the cache. Thus, a pool that
     * remains idle for long enough will not consume any resources. Note that pools with similar
     * properties but different details (for example, timeout parameters) may be created using
     * {@link ThreadPoolExecutor} constructors.
     *
     * @return the newly created thread pool
     */
    public static ExecutorService newCachedThreadPool(String name) {
        Preconditions.checkNotNull(name, "Name is required");
        Preconditions.checkArgument(!name.isEmpty(), "Name must not be empty");
        return newCachedThreadPoolWithMaxThreads(Short.MAX_VALUE, name);
    }

    /**
     * Creates a thread pool that creates new threads as needed, but will reuse previously
     * constructed threads when they are available, and uses the provided ThreadFactory to create
     * new threads when needed.
     *
     * @param threadFactory the factory to use when creating new threads
     * @return the newly created thread pool
     * @throws NullPointerException if threadFactory is null
     * @deprecated Please prefer {@link #newCachedThreadPool(String)}
     */
    @Deprecated
    public static ExecutorService newCachedThreadPool(ThreadFactory threadFactory) {
        return newCachedThreadPool(threadFactory, DEFAULT_THREAD_POOL_TIMEOUT_MILLIS);
    }

    /**
     * Creates a thread pool that creates new threads as needed, but will reuse previously
     * constructed threads when they are available, and uses the provided ThreadFactory to create
     * new threads when needed.
     * <p>
     * Important note: unless you know you have specific performance reasons for specifying a
     * timeout value, use newCachedThreadPool(threadFactory)
     *
     * @param threadFactory the factory to use when creating new threads
     * @return the newly created thread pool
     * @throws NullPointerException if threadFactory is null
     * @deprecated Please prefer {@link #newCachedThreadPool(String)}
     */
    @Deprecated
    public static ExecutorService newCachedThreadPool(ThreadFactory threadFactory, int threadTimeoutMillis) {
        return tryInstrumentCachedExecutor(
                newThreadPoolExecutor(
                        0,
                        Integer.MAX_VALUE,
                        threadTimeoutMillis,
                        TimeUnit.MILLISECONDS,
                        new SynchronousQueue<Runnable>(),
                        threadFactory),
                threadFactory);
    }

    /** Specialized cached executor which throws
     * {@link java.util.concurrent.RejectedExecutionException} once max-threads have been exceeded.
     *
     * If you have any doubt, this probably isn't what you're looking for. Best of luck, friend.
     */
    @Beta
    public static ExecutorService newCachedThreadPoolWithMaxThreads(int maxThreads, String name) {
        Preconditions.checkNotNull(name, "Name is required");
        Preconditions.checkArgument(!name.isEmpty(), "Name must not be empty");
        Preconditions.checkArgument(maxThreads > 0, "Max threads must be positive");
        return MetricRegistries.executor()
                .registry(SharedTaggedMetricRegistries.getSingleton())
                .name(name)
                .executor(PTExecutors.wrap(
                        name,
                        new AtlasRenamingExecutorService(
                                ViewExecutor.builder(SHARED_EXECUTOR.get())
                                        .setMaxSize(Math.min(Short.MAX_VALUE, maxThreads))
                                        .setQueueLimit(0)
                                        .setUncaughtHandler(AtlasUncaughtExceptionHandler.INSTANCE)
                                        .build(),
                                AtlasUncaughtExceptionHandler.INSTANCE,
                                AtlasRenamingExecutorService.threadNameSupplier(name))))
                // Unhelpful for cached executors
                .reportQueuedDuration(false)
                .build();
    }

    /**
     * Instruments the provided {@link ExecutorService} if the {@link ThreadFactory} is a {@link NamedThreadFactory}.
     */
    @SuppressWarnings("deprecation") // No reasonable way to pass a TaggedMetricRegistry
    private static ExecutorService tryInstrumentCachedExecutor(ExecutorService executorService, ThreadFactory factory) {
        if (factory instanceof NamedThreadFactory) {
            String name = ((NamedThreadFactory) factory).getPrefix();
            return MetricRegistries.executor()
                    .registry(SharedTaggedMetricRegistries.getSingleton())
                    .name(name)
                    .executor(executorService)
                    // Unhelpful for cached executors
                    .reportQueuedDuration(false)
                    .build();
        }
        return executorService;
    }

    /**
     * Creates a thread pool that reuses a fixed number of threads operating off a shared unbounded
     * queue.  At any point, at most <tt>numThreads</tt> threads will be active processing tasks.  If
     * additional tasks are submitted when all threads are active, they will wait in the queue until
     * a thread is available.  If any thread terminates due to a failure during execution prior to
     * shutdown, a new one will take its place if needed to execute subsequent tasks.  The threads
     * in the pool will exist until it is explicitly {@link
     * ExecutorService#shutdown shutdown}.
     *
     * @param numThreads the number of threads in the pool
     * @return the newly created thread pool
     * @throws IllegalArgumentException if <tt>numThreads &lt;= 0</tt>
     */
    public static ExecutorService newFixedThreadPool(int numThreads) {
        return newFixedThreadPool(numThreads, computeBaseThreadName());
    }

    /**
     * Creates a thread pool that reuses a fixed number of threads operating off a shared unbounded
     * queue, using the provided ThreadFactory to create new threads when needed.  At any point, at
     * most <tt>numThreads</tt> threads will be active processing tasks.  If additional tasks are
     * submitted when all threads are active, they will wait in the queue until a thread is
     * available.  If any thread terminates due to a failure during execution prior to shutdown, a
     * new one will take its place if needed to execute subsequent tasks.  The threads in the pool
     * will exist until it is explicitly {@link ExecutorService#shutdown}.
     *
     * @param numThreads the number of threads in the pool
     * @param threadFactory the factory to use when creating new threads
     * @return the newly created thread pool
     * @throws NullPointerException if threadFactory is null
     * @throws IllegalArgumentException if <tt>numThreads &lt;= 0</tt>
     * @deprecated Prefer {@link #newFixedThreadPool(int, String)}.
     */
    @Deprecated
    public static ThreadPoolExecutor newFixedThreadPool(int numThreads, ThreadFactory threadFactory) {
        return newThreadPoolExecutor(
                numThreads,
                numThreads,
                DEFAULT_THREAD_POOL_TIMEOUT_MILLIS,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                threadFactory);
    }

    /**
     * Creates a thread pool that reuses a fixed number of threads operating off a shared unbounded
     * queue.  At any point, at most <tt>numThreads</tt> threads will be active processing tasks.  If
     * additional tasks are submitted when all threads are active, they will wait in the queue until
     * a thread is available.  If any thread terminates due to a failure during execution prior to
     * shutdown, a new one will take its place if needed to execute subsequent tasks.  The threads
     * in the pool will exist until it is explicitly {@link
     * ExecutorService#shutdown shutdown}.
     *
     * @param numThreads the number of threads in the pool
     * @param name Executor name used for thread naming and instrumentation
     * @return the newly created thread pool
     * @throws IllegalArgumentException if <tt>numThreads &lt;= 0</tt>
     */
    public static ExecutorService newFixedThreadPool(int numThreads, String name) {
        return MetricRegistries.instrument(
                SharedTaggedMetricRegistries.getSingleton(),
                PTExecutors.wrap(
                        name,
                        new AtlasRenamingExecutorService(
                                ViewExecutor.builder(SHARED_EXECUTOR.get())
                                        .setMaxSize(Math.min(numThreads, Short.MAX_VALUE))
                                        .setQueueLimit(Integer.MAX_VALUE)
                                        .setUncaughtHandler(AtlasUncaughtExceptionHandler.INSTANCE)
                                        .build(),
                                AtlasUncaughtExceptionHandler.INSTANCE,
                                AtlasRenamingExecutorService.threadNameSupplier(name))),
                name);
    }

    /**
     * Creates a thread pool that can schedule commands to run after a given delay, or to execute
     * periodically.
     *
     * @param corePoolSize the number of threads to keep in the pool, even if they are idle.
     *
     * @return a newly created scheduled thread pool
     * @throws IllegalArgumentException if <tt>corePoolSize &lt; 0</tt>
     */
    public static ScheduledThreadPoolExecutor newScheduledThreadPool(int corePoolSize) {
        return newScheduledThreadPoolExecutor(corePoolSize, newNamedThreadFactory(true));
    }

    /**
     * Creates a thread pool that can schedule commands to run after a given delay, or to execute
     * periodically.
     *
     * @param corePoolSize the number of threads to keep in the pool, even if they are idle.
     * @param threadFactory the factory to use when the executor creates a new thread.
     * @return a newly created scheduled thread pool
     * @throws IllegalArgumentException if <tt>corePoolSize &lt; 0</tt>
     * @throws NullPointerException if threadFactory is null
     */
    public static ScheduledThreadPoolExecutor newScheduledThreadPool(int corePoolSize, ThreadFactory threadFactory) {
        return newScheduledThreadPoolExecutor(corePoolSize, threadFactory);
    }

    /**
     * Creates an Executor that uses a single worker thread operating off an unbounded queue. (Note
     * however that if this single thread terminates due to a failure during execution prior to
     * shutdown, a new one will take its place if needed to execute subsequent tasks.)  Tasks are
     * guaranteed to execute sequentially, and no more than one task will be active at any given
     * time. Unlike the otherwise equivalent <tt>newFixedThreadPool(1)</tt> the returned executor is
     * guaranteed not to be reconfigurable to use additional threads.
     *
     * @return the newly created single-threaded Executor
     */
    public static ExecutorService newSingleThreadExecutor() {
        return Executors.unconfigurableExecutorService(newFixedThreadPool(1));
    }

    /**
     * Creates an Executor that uses a single worker thread operating off an unbounded queue. (Note
     * however that if this single thread terminates due to a failure during execution prior to
     * shutdown, a new one will take its place if needed to execute subsequent tasks.)  Tasks are
     * guaranteed to execute sequentially, and no more than one task will be active at any given
     * time. Unlike the otherwise equivalent <tt>newFixedThreadPool(1)</tt> the returned executor is
     * guaranteed not to be reconfigurable to use additional threads.
     *
     * @param isDaemon denotes if the thread should be run as a daemon or not
     * @return the newly created single-threaded Executor
     */
    public static ExecutorService newSingleThreadExecutor(boolean isDaemon) {
        return Executors.unconfigurableExecutorService(
                isDaemon ? newFixedThreadPool(1) : newFixedThreadPool(1, newNamedThreadFactory(false)));
    }

    /**
     * Creates an Executor that uses a single worker thread operating off an unbounded queue, and
     * uses the provided ThreadFactory to create a new thread when needed. Unlike the otherwise
     * equivalent <tt>newFixedThreadPool(1, threadFactory)</tt> the returned executor is guaranteed
     * not to be reconfigurable to use additional threads.
     *
     * @param threadFactory the factory to use when creating new threads
     * @return the newly created single-threaded Executor
     * @throws NullPointerException if threadFactory is null
     * @deprecated Prefer {@link #newSingleThreadExecutor()}
     */
    @Deprecated
    public static ExecutorService newSingleThreadExecutor(ThreadFactory threadFactory) {
        return Executors.unconfigurableExecutorService(newFixedThreadPool(1, threadFactory));
    }

    /**
     * Creates a single-threaded executor that can schedule commands to run after a given delay, or
     * to execute periodically.  (Note however that if this single thread terminates due to a
     * failure during execution prior to shutdown, a new one will take its place if needed to
     * execute subsequent tasks.)  Tasks are guaranteed to execute sequentially, and no more than
     * one task will be active at any given time. Unlike the otherwise equivalent
     * <tt>newScheduledThreadPool(1)</tt> the returned executor is guaranteed not to be
     * reconfigurable to use additional threads.
     *
     * @return the newly created scheduled executor
     */
    public static ScheduledExecutorService newSingleThreadScheduledExecutor() {
        ThreadFactory factory = newNamedThreadFactory(true);
        String executorName = getExecutorName(factory);
        return wrap(executorName, Executors.newSingleThreadScheduledExecutor(factory));
    }

    /**
     * Creates a single-threaded executor that can schedule commands to run after a given delay, or
     * to execute periodically.  (Note however that if this single thread terminates due to a
     * failure during execution prior to shutdown, a new one will take its place if needed to
     * execute subsequent tasks.)  Tasks are guaranteed to execute sequentially, and no more than
     * one task will be active at any given time. Unlike the otherwise equivalent
     * <tt>newScheduledThreadPool(1, threadFactory)</tt> the returned executor is guaranteed not to
     * be reconfigurable to use additional threads.
     *
     * @param threadFactory the factory to use when creating new threads
     * @return a newly created scheduled executor
     * @throws NullPointerException if threadFactory is null
     */
    public static ScheduledExecutorService newSingleThreadScheduledExecutor(ThreadFactory threadFactory) {
        String executorName = getExecutorName(threadFactory);
        return wrap(executorName, Executors.newSingleThreadScheduledExecutor(threadFactory));
    }

    /**
     * Creates a new <tt>ThreadPoolExecutor</tt> with the given initial parameters and default
     * thread factory and rejected execution handler.  It may be more convenient to use one of the
     * {@link Executors} factory methods instead of this general purpose
     * constructor.
     *
     * @param corePoolSize the number of threads to keep in the pool, even if they are idle.
     * @param maximumPoolSize the maximum number of threads to allow in the pool.
     * @param keepAliveTime when the number of threads is greater than the core, this is the maximum
     *        time that excess idle threads will wait for new tasks before terminating.
     * @param unit the time unit for the keepAliveTime argument.
     * @param workQueue the queue to use for holding tasks before they are executed. This queue will
     *        hold only the <tt>Runnable</tt> tasks submitted by the <tt>execute</tt> method.
     * @throws IllegalArgumentException if corePoolSize or keepAliveTime less than zero, or if
     *         maximumPoolSize less than or equal to zero, or if corePoolSize greater than
     *         maximumPoolSize.
     * @throws NullPointerException if <tt>workQueue</tt> is null
     */
    public static ThreadPoolExecutor newThreadPoolExecutor(
            int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue) {
        return newThreadPoolExecutor(
                corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, newNamedThreadFactory(), defaultHandler);
    }

    /**
     * Creates a new <tt>ThreadPoolExecutor</tt> with the given initial parameters and default
     * rejected execution handler.
     *
     * @param corePoolSize the number of threads to keep in the pool, even if they are idle.
     * @param maximumPoolSize the maximum number of threads to allow in the pool.
     * @param keepAliveTime the maximum time that idle threads will wait for new tasks before
     *        terminating.
     * @param unit the time unit for the keepAliveTime argument.
     * @param workQueue the queue to use for holding tasks before they are executed. This queue will
     *        hold only the <tt>Runnable</tt> tasks submitted by the <tt>execute</tt> method.
     * @param threadFactory the factory to use when the executor creates a new thread.
     * @throws IllegalArgumentException if corePoolSize or keepAliveTime less than zero, or if
     *         maximumPoolSize less than or equal to zero, or if corePoolSize greater than
     *         maximumPoolSize.
     * @throws NullPointerException if <tt>workQueue</tt> or <tt>threadFactory</tt> are null.
     */
    public static ThreadPoolExecutor newThreadPoolExecutor(
            int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue,
            ThreadFactory threadFactory) {
        return newThreadPoolExecutor(
                corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, defaultHandler);
    }

    /**
     * Creates a new <tt>ThreadPoolExecutor</tt> with the given initial parameters and default
     * thread factory.
     *
     * @param corePoolSize the number of threads to keep in the pool, even if they are idle.
     * @param maximumPoolSize the maximum number of threads to allow in the pool.
     * @param keepAliveTime the maximum time that idle threads will wait for new tasks before
     *        terminating.
     * @param unit the time unit for the keepAliveTime argument.
     * @param workQueue the queue to use for holding tasks before they are executed. This queue will
     *        hold only the <tt>Runnable</tt> tasks submitted by the <tt>execute</tt> method.
     * @param handler the handler to use when execution is blocked because the thread bounds and
     *        queue capacities are reached.
     * @throws IllegalArgumentException if corePoolSize or keepAliveTime less than zero, or if
     *         maximumPoolSize less than or equal to zero, or if corePoolSize greater than
     *         maximumPoolSize.
     * @throws NullPointerException if <tt>workQueue</tt> or <tt>handler</tt> are null.
     */
    public static ThreadPoolExecutor newThreadPoolExecutor(
            int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue,
            RejectedExecutionHandler handler) {
        return newThreadPoolExecutor(
                corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, newNamedThreadFactory(), handler);
    }

    /**
     * Creates a new <tt>ThreadPoolExecutor</tt> with the given initial parameters.
     *
     * @param corePoolSize the number of threads to keep in the pool, even if they are idle.
     * @param maximumPoolSize the maximum number of threads to allow in the pool.
     * @param keepAliveTime the maximum time that idle threads will wait for new tasks before
     *        terminating.
     * @param unit the time unit for the keepAliveTime argument.
     * @param workQueue the queue to use for holding tasks before they are executed. This queue will
     *        hold only the <tt>Runnable</tt> tasks submitted by the <tt>execute</tt> method.
     * @param threadFactory the factory to use when the executor creates a new thread.
     * @param handler the handler to use when execution is blocked because the thread bounds and
     *        queue capacities are reached.
     * @throws IllegalArgumentException if corePoolSize or keepAliveTime less than zero, or if
     *         maximumPoolSize less than or equal to zero, or if corePoolSize greater than
     *         maximumPoolSize.
     * @throws NullPointerException if <tt>workQueue</tt> or <tt>threadFactory</tt> or
     *         <tt>handler</tt> are null.
     */
    @SuppressWarnings("DangerousThreadPoolExecutorUsage")
    public static ThreadPoolExecutor newThreadPoolExecutor(
            int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue,
            ThreadFactory threadFactory,
            RejectedExecutionHandler handler) {
        String executorName = getExecutorName(threadFactory);
        ThreadPoolExecutor tpe =
                new ThreadPoolExecutor(
                        corePoolSize,
                        maximumPoolSize,
                        keepAliveTime, // (authorized)
                        unit,
                        workQueue,
                        threadFactory,
                        handler) {
                    @Override
                    public void execute(Runnable command) {
                        super.execute(wrap(executorName, command));
                    }
                };
        // QA-49019 - always allow core pool threads to timeout.
        if (keepAliveTime > 0) {
            tpe.allowCoreThreadTimeOut(true);
        }
        return tpe;
    }

    /**
     * Creates a new ScheduledThreadPoolExecutor with the given core pool size.
     *
     * @param corePoolSize the number of threads to keep in the pool, even if they are idle
     * @throws IllegalArgumentException if <tt>corePoolSize &lt; 0</tt>
     */
    public static ScheduledThreadPoolExecutor newScheduledThreadPoolExecutor(int corePoolSize) {
        return newScheduledThreadPoolExecutor(corePoolSize, newNamedThreadFactory(true), defaultHandler);
    }

    /**
     * Creates a new ScheduledThreadPoolExecutor with the given initial parameters.
     *
     * @param corePoolSize the number of threads to keep in the pool, even if they are idle
     * @param threadFactory the factory to use when the executor creates a new thread
     * @throws IllegalArgumentException if <tt>corePoolSize &lt; 0</tt>
     * @throws NullPointerException if threadFactory is null
     */
    public static ScheduledThreadPoolExecutor newScheduledThreadPoolExecutor(
            int corePoolSize, ThreadFactory threadFactory) {
        return newScheduledThreadPoolExecutor(corePoolSize, threadFactory, defaultHandler);
    }

    /**
     * Creates a new ScheduledThreadPoolExecutor with the given initial parameters.
     *
     * @param corePoolSize the number of threads to keep in the pool, even if they are idle
     * @param handler the handler to use when execution is blocked because the thread bounds and
     *        queue capacities are reached
     * @throws IllegalArgumentException if <tt>corePoolSize &lt; 0</tt>
     * @throws NullPointerException if handler is null
     */
    public static ScheduledThreadPoolExecutor newScheduledThreadPoolExecutor(
            int corePoolSize, RejectedExecutionHandler handler) {
        return newScheduledThreadPoolExecutor(corePoolSize, newNamedThreadFactory(true), handler);
    }

    /**
     * Creates a new ScheduledThreadPoolExecutor with the given initial parameters.
     *
     * @param corePoolSize the number of threads to keep in the pool, even if they are idle
     * @param threadFactory the factory to use when the executor creates a new thread
     * @param handler the handler to use when execution is blocked because the thread bounds and
     *        queue capacities are reached.
     * @throws IllegalArgumentException if <tt>corePoolSize &lt; 0</tt>
     * @throws NullPointerException if threadFactory or handler is null
     */
    @SuppressWarnings("DangerousThreadPoolExecutorUsage")
    public static ScheduledThreadPoolExecutor newScheduledThreadPoolExecutor(
            int corePoolSize, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
        Preconditions.checkArgument(
                corePoolSize >= 0,
                "Cannot create a ScheduledThreadPoolExecutor with %s threads - thread count must not be negative!",
                SafeArg.of("corePoolSize", corePoolSize));
        int positiveCorePoolSize = corePoolSize > 0 ? corePoolSize : 1;
        String executorName = getExecutorName(threadFactory);
        ScheduledThreadPoolExecutor ret =
                new ScheduledThreadPoolExecutor(positiveCorePoolSize, threadFactory, handler) {
                    @Override
                    public void execute(Runnable command) {
                        super.execute(wrap(executorName, command));
                    }
                };
        ret.setKeepAliveTime(DEFAULT_THREAD_POOL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        return ret;
    }

    // Characters likely to be part of a pattern, not part of the executor name. For example, rather than
    // "mine-bitcoin-31" we want "mine-bitcoin"
    private static final CharMatcher THREAD_NAME_TRIMMED_CHARS =
            CharMatcher.inRange('0', '9').or(CharMatcher.anyOf(".-_")).or(CharMatcher.whitespace());

    @VisibleForTesting
    static String getExecutorName(ThreadFactory factory) {
        if (factory instanceof NamedThreadFactory) {
            return ((NamedThreadFactory) factory).getPrefix();
        }
        // fall back to create a thread, and capture the name
        // Thread isn't started, allow it to be collected immediately.
        String threadName = factory.newThread(Runnables.doNothing()).getName();
        if (!Strings.isNullOrEmpty(threadName)) {
            String trimmed = THREAD_NAME_TRIMMED_CHARS.trimTrailingFrom(threadName);
            if (!Strings.isNullOrEmpty(trimmed)) {
                return trimmed;
            }
        }
        return "PTExecutor";
    }

    /**
     * Wraps the given {@code ExecutorService} so that {@link ExecutorInheritableThreadLocal}
     * variables are propagated through.
     */
    public static ExecutorService wrap(final String operationName, final ExecutorService executorService) {
        return new AbstractForwardingExecutorService() {
            @Override
            protected ExecutorService delegate() {
                return executorService;
            }

            @Override
            protected Runnable wrap(Runnable runnable) {
                return PTExecutors.wrap(operationName, runnable);
            }
        };
    }

    public static ExecutorService wrap(final ExecutorService executorService) {
        return wrap("PTExecutor", executorService);
    }

    /**
     * Wraps the given {@code ScheduledExecutorService} so that {@link
     * ExecutorInheritableThreadLocal} variables are propagated through.
     */
    public static ScheduledExecutorService wrap(
            final String operationName, final ScheduledExecutorService scheduledExecutorService) {
        return new InternalForwardingScheduledExecutorService() {
            @Override
            protected ScheduledExecutorService delegate() {
                return scheduledExecutorService;
            }

            @Override
            protected Runnable wrap(Runnable runnable) {
                return PTExecutors.wrap(operationName, runnable);
            }

            @Override
            protected <T> Callable<T> wrap(Callable<T> callable) {
                return PTExecutors.wrap(operationName, callable);
            }

            @Override
            protected Runnable wrapRecurring(Runnable runnable) {
                // Intentionally does not retain current thread state.
                return Tracers.wrapWithNewTrace(operationName, runnable);
            }
        };
    }

    public static ScheduledExecutorService wrap(final ScheduledExecutorService scheduledExecutorService) {
        return wrap("PTExecutor", scheduledExecutorService);
    }

    /**
     * Wraps the given {@code Runnable} so that {@link ExecutorInheritableThreadLocal} variables are
     * propagated through.  If {@code runnable} implements the {@link Future}
     * interface, then the returned {@code Runnable} will also implement {@code Future}.
     */
    public static Runnable wrap(final Runnable runnable) {
        return wrap("PTExecutor", runnable);
    }

    public static Runnable wrap(final String operationName, final Runnable runnable) {
        Runnable wrappedRunnable = wrapRunnable(operationName, runnable);
        if (runnable instanceof Future<?>) {
            @SuppressWarnings("unchecked")
            Future<Object> unsafeFuture = (Future<Object>) runnable;
            return new ForwardingRunnableFuture<>(wrappedRunnable, unsafeFuture);
        }
        return wrappedRunnable;
    }

    public static <T> RunnableFuture<T> wrap(RunnableFuture<T> rf) {
        Runnable wrappedRunnable = wrapRunnable("PTExecutor", rf);
        return new ForwardingRunnableFuture<>(wrappedRunnable, rf);
    }

    /**
     * Wraps the given {@code Callable} so that {@link ExecutorInheritableThreadLocal} variables are
     * propagated through.
     */
    public static <T> Callable<T> wrap(final String operationName, final Callable<? extends T> callable) {
        final Map<WeakReference<? extends ExecutorInheritableThreadLocal<?>>, Object> mapForNewThread =
                ExecutorInheritableThreadLocal.getMapForNewThread();
        return Tracers.wrap(operationName, () -> {
            ConcurrentMap<WeakReference<? extends ExecutorInheritableThreadLocal<?>>, Object> oldMap =
                    ExecutorInheritableThreadLocal.installMapOnThread(mapForNewThread);
            try {
                return callable.call();
            } finally {
                ExecutorInheritableThreadLocal.uninstallMapOnThread(oldMap);
            }
        });
    }

    public static <T> Callable<T> wrap(final Callable<? extends T> callable) {
        return wrap("PTExecutor", callable);
    }
    /**
     * Wraps the given {@code Callable}s so that {@link ExecutorInheritableThreadLocal} variables
     * are propagated through.
     */
    public static <T> Collection<Callable<T>> wrap(Collection<? extends Callable<? extends T>> tasks) {
        Collection<Callable<T>> wrapped = new ArrayList<Callable<T>>(tasks.size());
        for (Callable<? extends T> task : tasks) {
            wrapped.add(wrap(task));
        }
        return wrapped;
    }

    private static Runnable wrapRunnable(String operationName, Runnable runnable) {
        final Map<WeakReference<? extends ExecutorInheritableThreadLocal<?>>, Object> mapForNewThread =
                ExecutorInheritableThreadLocal.getMapForNewThread();

        return Tracers.wrap(operationName, () -> {
            ConcurrentMap<WeakReference<? extends ExecutorInheritableThreadLocal<?>>, Object> oldMap =
                    ExecutorInheritableThreadLocal.installMapOnThread(mapForNewThread);
            try {
                runnable.run();
            } finally {
                ExecutorInheritableThreadLocal.uninstallMapOnThread(oldMap);
            }
        });
    }

    static String computeBaseThreadName() {
        return computeBaseThreadName(null);
    }

    static String computeBaseThreadName(@Nullable Class<?> classToIgnore) {
        String fileNameToIgnore = (classToIgnore == null) ? null : classToIgnore.getSimpleName() + ".java";
        StackTraceElement[] stackTrace = new Throwable().getStackTrace();
        if (stackTrace != null) {
            for (StackTraceElement stackTraceElement : stackTrace) {
                String fileName = stackTraceElement.getFileName();
                if ((fileName != null)
                        && !fileName.equals(FILE_NAME_FOR_THIS_CLASS)
                        && !fileName.equals(fileNameToIgnore)) {
                    return fileName + ":" + stackTraceElement.getLineNumber();
                }
            }
        }
        log.warn("Can't figure out what name to use for this thread factory!");
        return "Unnamed thread";
    }

    public static ThreadFactory newNamedThreadFactory() {
        return newNamedThreadFactory(true, null);
    }

    public static ThreadFactory newNamedThreadFactory(boolean isDaemon) {
        return newNamedThreadFactory(isDaemon, null);
    }

    public static ThreadFactory newNamedThreadFactory(boolean isDaemon, Class<?> classToIgnore) {
        return new NamedThreadFactory(computeBaseThreadName(classToIgnore), isDaemon);
    }

    public static ThreadFactory newThreadFactory(final String prefix, final int priority, final boolean isDaemon) {
        ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicInteger nextThreadId = new AtomicInteger();

            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable, prefix + "-" + nextThreadId.getAndIncrement());
                thread.setPriority(priority);
                thread.setDaemon(isDaemon);
                return thread;
            }
        };

        return threadFactory;
    }

    private PTExecutors() {
        throw new AssertionError("uninstantiable");
    }
}

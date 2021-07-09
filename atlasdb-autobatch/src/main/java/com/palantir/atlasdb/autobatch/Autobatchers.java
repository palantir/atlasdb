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

package com.palantir.atlasdb.autobatch;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.errorprone.annotations.CompileTimeConstant;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WaitStrategy;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.tracing.Observability;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Autobatchers {
    private static final Logger log = LoggerFactory.getLogger(Autobatchers.class);

    private static final int DEFAULT_BUFFER_SIZE = 4096;

    /**
     * When invoking an {@link DisruptorAutobatcher autobatcher}, an argument needs to be supplied. In the case of
     * {@link CoalescingRequestSupplier}, this means we need to pass in a placeholder value.
     * {@link SupplierKey#INSTANCE} is this placeholder value.
     */
    public enum SupplierKey {
        INSTANCE;

        static <T> Map<SupplierKey, T> wrap(T object) {
            return ImmutableMap.of(INSTANCE, object);
        }
    }

    private Autobatchers() {}

    /**
     * This coalesces requests for the same input to the same output. {@link #independent} differs from this method
     * because the same semantic request will be processed independently.
     *
     * @param function function that processes a batch ({@link java.util.Set}) of {@code I}, producing a mapping from
     * {@code I} to {@code O}
     * @param <I> type of input element
     * @param <O> type of output element
     * @return builder where the autobatcher can be further customised
     * @see CoalescingRequestFunction
     * @see CoalescingRequestConsumer
     */
    public static <I, O> AutobatcherBuilder<I, O> coalescing(CoalescingRequestFunction<I, O> function) {
        return new AutobatcherBuilder<>(parameters -> new CoalescingBatchingEventHandler<>(
                maybeWrapWithTimeout(function, parameters), parameters.batchSize()));
    }

    public static <O> AutobatcherBuilder<SupplierKey, O> coalescing(Supplier<O> supplier) {
        return coalescing(new CoalescingRequestSupplier<>(supplier));
    }

    /**
     * This builds an autobatcher where the function takes a batch of elements (equal elements are considered
     * independently) and completes the futures associated with each request. This is different from {@link #coalescing}
     * as each input element even if identical, is processed independently and separately.
     *
     * @param batchFunction consumer that processes a batch of input elements and resolves the associated futures with
     * the result for that input element
     * @param <I> type of input element
     * @param <O> type of output element
     * @return builder where the autobatcher can be further customised
     */
    public static <I, O> AutobatcherBuilder<I, O> independent(Consumer<List<BatchElement<I, O>>> batchFunction) {
        return new AutobatcherBuilder<>(parameters -> new IndependentBatchingEventHandler<>(
                maybeWrapWithTimeout(batchFunction, parameters), parameters.batchSize()));
    }

    private static <I, O> Consumer<List<BatchElement<I, O>>> maybeWrapWithTimeout(
            Consumer<List<BatchElement<I, O>>> batchFunction, EventHandlerParameters parameters) {
        return parameters
                .batchFunctionTimeout()
                .map(timeout -> wrapWithTimeout(batchFunction, parameters.safeLoggablePurpose(), timeout))
                .orElse(batchFunction);
    }

    private static <I, O> CoalescingRequestFunction<I, O> maybeWrapWithTimeout(
            CoalescingRequestFunction<I, O> coalescingFunction, EventHandlerParameters parameters) {
        return parameters
                .batchFunctionTimeout()
                .map(timeout -> wrapWithTimeout(coalescingFunction, parameters.safeLoggablePurpose(), timeout))
                .orElse(coalescingFunction);
    }

    private static <I, O> Consumer<List<BatchElement<I, O>>> wrapWithTimeout(
            Consumer<List<BatchElement<I, O>>> delegate, String safeLoggablePurpose, Duration maxRuntime) {
        TimeLimiter limiter = createTimeLimiter(safeLoggablePurpose);
        return elements -> {
            try {
                limiter.runWithTimeout(() -> delegate.accept(elements), maxRuntime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (TimeoutException e) {
                log.info(
                        "Autobatcher timed out and request had to be interrupted",
                        SafeArg.of("safeLoggablePurpose", safeLoggablePurpose),
                        e);
                throw new RuntimeException(e);
            } catch (UncheckedExecutionException e) {
                Throwables.throwIfUnchecked(e.getCause());
                throw new RuntimeException(e.getCause());
            }
        };
    }

    private static <I, O> CoalescingRequestFunction<I, O> wrapWithTimeout(
            CoalescingRequestFunction<I, O> delegate, String safeLoggablePurpose, Duration maxRuntime) {
        TimeLimiter limiter = createTimeLimiter(safeLoggablePurpose);
        return elements -> {
            try {
                return limiter.callWithTimeout(() -> delegate.apply(elements), maxRuntime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (TimeoutException e) {
                log.info(
                        "Autobatcher timed out and request had to be interrupted",
                        SafeArg.of("safeLoggablePurpose", safeLoggablePurpose),
                        e);
                throw new RuntimeException(e);
            } catch (ExecutionException | UncheckedExecutionException e) {
                Throwables.throwIfUnchecked(e.getCause());
                throw new RuntimeException(e.getCause());
            }
        };
    }

    private static TimeLimiter createTimeLimiter(String safeLoggablePurpose) {
        return SimpleTimeLimiter.create(
                PTExecutors.newCachedThreadPool("autobatcher." + safeLoggablePurpose + "-timeout"));
    }

    public static final class AutobatcherBuilder<I, O> {

        private final Function<EventHandlerParameters, EventHandler<BatchElement<I, O>>> handlerFactory;
        private final ImmutableMap.Builder<String, String> safeTags = ImmutableMap.builder();

        private Observability observability = Observability.UNDECIDED;
        private OptionalInt bufferSize = OptionalInt.empty();
        private Optional<Duration> batchFunctionTimeout = Optional.empty();
        private Optional<WaitStrategy> waitStrategy = Optional.empty();

        @Nullable
        private String purpose;

        private AutobatcherBuilder(Function<EventHandlerParameters, EventHandler<BatchElement<I, O>>> handlerFactory) {
            this.handlerFactory = handlerFactory;
        }

        public AutobatcherBuilder<I, O> safeLoggablePurpose(@CompileTimeConstant String purposeParam) {
            this.purpose = purposeParam;
            return this;
        }

        public AutobatcherBuilder<I, O> safeTag(String key, String value) {
            this.safeTags.put(key, value);
            return this;
        }

        public AutobatcherBuilder<I, O> observability(Observability observabilityParam) {
            this.observability = observabilityParam;
            return this;
        }

        public AutobatcherBuilder<I, O> bufferSize(OptionalInt bufferSizeParam) {
            this.bufferSize = bufferSizeParam;
            return this;
        }

        public AutobatcherBuilder<I, O> waitStrategy(WaitStrategy waitStrategyParam) {
            this.waitStrategy = Optional.of(waitStrategyParam);
            return this;
        }

        public AutobatcherBuilder<I, O> batchFunctionTimeout(Duration duration) {
            this.batchFunctionTimeout = Optional.of(duration);
            return this;
        }

        public DisruptorAutobatcher<I, O> build() {
            Preconditions.checkArgument(purpose != null, "purpose must be provided");

            ImmutableEventHandlerParameters.Builder parametersBuilder = ImmutableEventHandlerParameters.builder();
            bufferSize.ifPresent(parametersBuilder::batchSize);
            parametersBuilder.safeLoggablePurpose(purpose).batchFunctionTimeout(batchFunctionTimeout);
            EventHandlerParameters parameters = parametersBuilder.build();

            EventHandler<BatchElement<I, O>> handler = this.handlerFactory.apply(parameters);

            EventHandler<BatchElement<I, O>> tracingHandler =
                    new TracingEventHandler<>(handler, parameters.batchSize());

            EventHandler<BatchElement<I, O>> profiledHandler =
                    new ProfilingEventHandler<>(tracingHandler, purpose, safeTags.build());

            return DisruptorAutobatcher.create(profiledHandler, parameters.batchSize(), purpose, waitStrategy);
        }
    }

    @Value.Immutable
    interface EventHandlerParameters {
        @Value.Default
        default int batchSize() {
            return DEFAULT_BUFFER_SIZE;
        }

        String safeLoggablePurpose();

        Optional<Duration> batchFunctionTimeout();
    }
}

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

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CompileTimeConstant;
import com.lmax.disruptor.EventHandler;
import com.palantir.logsafe.Preconditions;
import com.palantir.tracing.Observability;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;

public final class Autobatchers {

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
        return new AutobatcherBuilder<>(bufferSize -> new CoalescingBatchingEventHandler<>(function, bufferSize));
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
     * @return builder where the autobatch can be further customised
     */
    public static <I, O> AutobatcherBuilder<I, O> independent(Consumer<List<BatchElement<I, O>>> batchFunction) {
        return new AutobatcherBuilder<>(bufferSize -> new IndependentBatchingEventHandler<>(batchFunction, bufferSize));
    }

    public static final class AutobatcherBuilder<I, O> {

        private final Function<Integer, EventHandler<BatchElement<I, O>>> handlerFactory;
        private final ImmutableMap.Builder<String, String> safeTags = ImmutableMap.builder();

        private Observability observability = Observability.UNDECIDED;
        @Nullable private String purpose;

        private AutobatcherBuilder(Function<Integer, EventHandler<BatchElement<I, O>>> handlerFactory) {
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

        public DisruptorAutobatcher<I, O> build() {
            Preconditions.checkArgument(purpose != null, "purpose must be provided");
            EventHandler<BatchElement<I, O>> handler = this.handlerFactory.apply(DEFAULT_BUFFER_SIZE);

            EventHandler<BatchElement<I, O>> tracingHandler =
                    new TracingEventHandler<>(handler, DEFAULT_BUFFER_SIZE);

            EventHandler<BatchElement<I, O>> profiledHandler =
                    new ProfilingEventHandler<>(tracingHandler, purpose, safeTags.build());

            return DisruptorAutobatcher.create(profiledHandler, DEFAULT_BUFFER_SIZE, purpose);
        }

    }

}

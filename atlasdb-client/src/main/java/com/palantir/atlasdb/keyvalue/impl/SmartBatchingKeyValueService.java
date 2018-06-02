/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.immutables.value.Value;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.dsl.Disruptor;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Write;

/**
 * Batch together multiple mutations to the KVS.
 * <p>
 * This helps with all kinds of KVS communication; underlying implementations do their own batching for optimal
 * throughput so this class helps produce more optimal batches there. Additionally, we know that some KVSes can
 * contend between mutations (in Cassandra's case, between mutations to the same row), and so this technique can
 * help remove such issues.
 * <p>
 * The implementation uses LMAX's 'disruptor' to reduce overheads.
 */
public class SmartBatchingKeyValueService extends ForwardingKeyValueService {
    private static final EventTranslatorTwoArg<WritesHolder, CompletableFuture<?>, Stream<Write>> EVENT_TRANSLATOR =
            (writesHolder, sequence, completion, writes) -> writesHolder.setCompletion(completion).setWrites(writes);

    private final KeyValueService delegate;
    private final Disruptor<WritesHolder> disruptor;

    private SmartBatchingKeyValueService(
            KeyValueService delegate,
            Disruptor<WritesHolder> disruptor) {
        this.delegate = delegate;
        this.disruptor = disruptor;
    }

    @Override
    protected KeyValueService delegate() {
        return delegate;
    }

    @Override
    public void close() {
        super.close();
        disruptor.shutdown();
    }

    @Override
    public void put(Stream<Write> writes) throws KeyAlreadyExistsException {
        CompletableFuture<?> result = new CompletableFuture<>();
        disruptor.publishEvent(EVENT_TRANSLATOR, result, writes);
        try {
            result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof KeyAlreadyExistsException) {
                throw new KeyAlreadyExistsException(e.getCause().getMessage(), e);
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    @Value.Modifiable
    interface WritesHolder {
        @Nullable
        Stream<Write> getWrites();
        WritesHolder setWrites(Stream<Write> writes);

        @Nullable
        CompletableFuture<?> getCompletion();
        WritesHolder setCompletion(CompletableFuture<?> completion);
    }

    private static class WriteHolderHandler implements EventHandler<WritesHolder> {
        private final KeyValueService delegate;
        private final List<WritesHolder> batch = new ArrayList<>();

        private WriteHolderHandler(KeyValueService delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onEvent(WritesHolder event, long sequence, boolean endOfBatch) {
            batch.add(ModifiableWritesHolder.create().from(event));
            event.setCompletion(null);
            event.setWrites(null);
            if (endOfBatch) {
                flush();
            }
        }

        private void flush() {
            try {
                delegate.put(batch.stream().flatMap(WritesHolder::getWrites));
                batch.stream().map(WritesHolder::getCompletion).forEach(c -> c.complete(null));
            } catch (Throwable t) {
                batch.stream().map(WritesHolder::getCompletion).forEach(c -> c.completeExceptionally(t));
            } finally {
                batch.clear();
            }
        }
    }

    public static KeyValueService create(KeyValueService delegate) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("smart-batching-kvs-%d")
                .build();

        // Given we batch at a lower level too, we are CPU limiting ourselves here. So, create a thread per CPU.
        int concurrency = Runtime.getRuntime().availableProcessors();
        int bufferSize = 1024;
        Disruptor<WritesHolder> disruptor = new Disruptor<>(ModifiableWritesHolder::create, bufferSize, threadFactory);

        WriteHolderHandler[] handlers = IntStream.range(0, concurrency)
                .mapToObj(unused -> new WriteHolderHandler(delegate))
                .toArray(WriteHolderHandler[]::new);

        disruptor.handleEventsWith(handlers);
        disruptor.start();

        return new SmartBatchingKeyValueService(delegate, disruptor);
    }
}

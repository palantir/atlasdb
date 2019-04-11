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

import static com.google.common.base.Preconditions.checkState;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * While this class is public, it shouldn't be used as API outside of AtlasDB because we
 * don't guarantee we won't break it.
 *
 * This is a smart version of {@link DisruptorAutobatcher} that batches tasks together and submits them to workers
 * in a worker pool. This may be used to avoid head-of-line blocking issues where a request is nondeterministically
 * slow.
 */
public final class MultisinkAutobatcher<T, R>
        implements AsyncFunction<T, R>, Function<T, ListenableFuture<R>>, Closeable {
    private static final int BUFFER_SIZE = 1024;
    private static final ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("multisink-autobatcher-%d")
            .build();

    private final DisruptorAutobatcher<T, R> batchCreator;
    private final Disruptor<List<BatchElement<T, R>>> batchElementDisruptor;

    private volatile boolean closed = false;

    private MultisinkAutobatcher(DisruptorAutobatcher<T, R> batchCreator,
            Disruptor<List<BatchElement<T, R>>> batchElementDisruptor) {
        this.batchCreator = batchCreator;
        this.batchElementDisruptor = batchElementDisruptor;
    }

    public static <T, R> MultisinkAutobatcher<T, R> create(Consumer<List<BatchElement<T, R>>> batchFunction) {
        Disruptor<List<BatchElement<T, R>>> batchElementDisruptor = new Disruptor<>(
                ArrayList::new, BUFFER_SIZE, threadFactory);
        DisruptorAutobatcher<T, R> batchingDisruptor = DisruptorAutobatcher.create(list -> {
            batchElementDisruptor.publishEvent(((event, sequence) -> {
                System.out.println("PUBLISH" + list);
                event.addAll(list.stream()
                        .map(x -> {
                            DefaultBatchElement<T, R> batchElement = new DefaultBatchElement<>();
                            batchElement.result = x.result();
                            batchElement.argument = x.argument();
                            return batchElement;
                        })
                        .collect(Collectors.toList()));
            }));
        });
        batchElementDisruptor.handleEventsWithWorkerPool(batchFunction::accept,
                batchFunction::accept,
                batchFunction::accept,
                batchFunction::accept);
        batchElementDisruptor.start();
        return new MultisinkAutobatcher<>(batchingDisruptor, batchElementDisruptor);
    }

    @Override
    public ListenableFuture<R> apply(T argument) {
        checkState(!closed, "Autobatcher is already shut down");
        return batchCreator.apply(argument);
    }

    @Override
    public void close() {
        closed = true;
        batchCreator.close();
        batchElementDisruptor.shutdown();
    }

    private static final class DefaultBatchElement<T, R> implements BatchElement<T, R> {
        private T argument;
        private SettableFuture<R> result;

        @Override
        public T argument() {
            return argument;
        }

        @Override
        public SettableFuture<R> result() {
            return result;
        }

        @Override
        public String toString() {
            return "DefaultBatchElement{" +
                    "argument=" + argument +
                    ", result=" + result +
                    '}';
        }
    }
}

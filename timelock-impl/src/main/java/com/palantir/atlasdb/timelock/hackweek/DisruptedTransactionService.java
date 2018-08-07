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

package com.palantir.atlasdb.timelock.hackweek;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.ThreadFactory;
import java.util.function.BiConsumer;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

public final class DisruptedTransactionService {
    private static final int BUFFER_SIZE = 1024;
    private final RingBuffer<Message> messages;

    private DisruptedTransactionService(RingBuffer<Message> messages) {
        this.messages = messages;
    }

    private static final class Message {
        private ByteBuffer message;
        private BiConsumer<Optional<ByteBuffer>, Throwable> responseConsumer;
    }

    public void enqueue(ByteBuffer message, BiConsumer<Optional<ByteBuffer>, Throwable> responseConsumer) {
        messages.publishEvent((event, sequence) -> {
            event.message = message;
            event.responseConsumer = responseConsumer;
        });
    }

    public static DisruptedTransactionService create(String name) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("transaction-service-" + name + "-%d")
                .build();
        ProtobufTransactionService txnService = new ProtobufTransactionService(new DefaultTransactionService());
        Disruptor<Message> disruptor = new Disruptor<>(
                Message::new, BUFFER_SIZE, threadFactory, ProducerType.SINGLE, new BlockingWaitStrategy());
        disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
            BiConsumer<Optional<ByteBuffer>, Throwable> responseConsumer = event.responseConsumer;
            ByteBuffer message = event.message;
            event.message = null;
            event.responseConsumer = null;
            Futures.addCallback(txnService.process(message), new FutureCallback<Optional<ByteBuffer>>() {
                @Override
                public void onSuccess(Optional<ByteBuffer> result) {
                    event.responseConsumer.accept(result, null);
                }

                @Override
                public void onFailure(Throwable throwable) {
                    event.responseConsumer.accept(null, throwable);
                }
            }, MoreExecutors.directExecutor());
        });

        return new DisruptedTransactionService(disruptor.getRingBuffer());
    }
}

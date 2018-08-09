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

import org.checkerframework.checker.nullness.compatqual.NullableDecl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.palantir.atlasdb.protos.generated.TransactionService.TransactionServiceRequest;

public final class DisruptedTransactionService {
    private static final Logger log = LoggerFactory.getLogger(DisruptedTransactionService.class);
    private static final int BUFFER_SIZE = 1024;
    private final RingBuffer<Message> messages;

    private DisruptedTransactionService(RingBuffer<Message> messages) {
        this.messages = messages;
    }

    private static final class Message {
        private TransactionServiceRequest message;
        private BiConsumer<Optional<ByteBuffer>, Throwable> responseConsumer;
    }

    private static final class Message2 {
        private ListenableFuture<Optional<GeneratedMessageV3>> result;
        private BiConsumer<Optional<ByteBuffer>, Throwable> responseConsumer;
    }

    public void enqueue(byte[] message, BiConsumer<Optional<ByteBuffer>, Throwable> responseConsumer) {
        try {
            TransactionServiceRequest parsed = TransactionServiceRequest.parseFrom(message);
            messages.publishEvent((event, sequence) -> {
                event.message = parsed;
                event.responseConsumer = responseConsumer;
            });
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public static DisruptedTransactionService create(String name) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("transaction-service-" + name + "-%d")
                .build();
        ProtobufTransactionService txnService = new ProtobufTransactionService(new DefaultTransactionService());

        Disruptor<Message2> responseDisruptor = new Disruptor<>(Message2::new, BUFFER_SIZE, threadFactory,
                ProducerType.SINGLE, new BlockingWaitStrategy());
        responseDisruptor.handleEventsWith((event, sequence, endOfBatch) ->
            Futures.addCallback(event.result, new FutureCallback<Optional<GeneratedMessageV3>>() {
                @Override
                public void onSuccess(@NullableDecl Optional<GeneratedMessageV3> result) {
                    Optional<ByteBuffer> res = result.map(GeneratedMessageV3::toByteString).map(ByteString::asReadOnlyByteBuffer);
                    event.responseConsumer.accept(res, null);
                }

                @Override
                public void onFailure(Throwable t) {
                    event.responseConsumer.accept(null, t);
                }
            }));

        RingBuffer<Message2> responseBuffer = responseDisruptor.start();

        Disruptor<Message> disruptor = new Disruptor<>(Message::new, BUFFER_SIZE, threadFactory);
        disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
            TransactionServiceRequest message = event.message;
            BiConsumer<Optional<ByteBuffer>, Throwable> responseConsumer = event.responseConsumer;
            event.message = null;
            event.responseConsumer = null;

            ListenableFuture<Optional<GeneratedMessageV3>> res = txnService.process(message);
            responseBuffer.publishEvent((msg, sq) -> {
                msg.responseConsumer = responseConsumer;
                msg.result = res;
            });
        });
        return new DisruptedTransactionService(disruptor.start());
    }
}

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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.palantir.atlasdb.protos.generated.TransactionService;
import com.palantir.atlasdb.protos.generated.TransactionService.TimestampRange;

public final class AutoBatchingTransactionServiceClient implements JamesTransactionService {
    private static final int BUFFER_SIZE = 1024;
    private final JamesTransactionService delegate;
    private final RingBuffer<PendingStart> pendingStartBuffer;
    private final RingBuffer<Unlock> unlockBuffer;

    private AutoBatchingTransactionServiceClient(JamesTransactionService delegate,
            RingBuffer<PendingStart> pendingStartBuffer,
            RingBuffer<Unlock> unlockBuffer) {
        this.delegate = delegate;
        this.pendingStartBuffer = pendingStartBuffer;
        this.unlockBuffer = unlockBuffer;
    }

    @Override
    public TransactionService.ImmutableTimestamp getImmutableTimestamp() {
        return delegate.getImmutableTimestamp();
    }

    @Override
    public TransactionService.Timestamp getFreshTimestamp() {
        return delegate.getFreshTimestamp();
    }

    @Override
    public TimestampRange startTransactions(long cachedUpTo, long numberOfTransactions) {
        CompletableFuture<TimestampRange> future = new CompletableFuture<>();
        pendingStartBuffer.publishEvent((event, sequence) -> {
            event.cachedUpTo = cachedUpTo;
            event.result = future;
            event.numberOfTransactions = numberOfTransactions;
        });
        return future.join();
    }

    @Override
    public TransactionService.CommitWritesResponse commitWrites(long startTimestamp,
            List<TransactionService.TableCell> writes) {
        return delegate.commitWrites(startTimestamp, writes);
    }

    @Override
    public TransactionService.CheckReadConflictsResponse checkReadConflicts(long startTimestamp,
            List<TransactionService.TableCell> reads, List<TransactionService.TableRange> ranges) {
        return delegate.checkReadConflicts(startTimestamp, reads, ranges);
    }

    @Override
    public ListenableFuture<?> waitForCommit(List<Long> startTimestamp) {
        return delegate.waitForCommit(startTimestamp);
    }

    @Override
    public void unlock(List<Long> startTimestamps) {
        unlockBuffer.publishEvent((event, sequence) -> event.startTimestamps = startTimestamps);
    }

    public static JamesTransactionService create(JamesTransactionService delegate) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("autobatching-transaction-service--%d")
                .build();

        Disruptor<PendingStart> txnStartDisruptor = new Disruptor<>(PendingStart::new, BUFFER_SIZE, threadFactory);
        txnStartDisruptor.handleEventsWith(new EventHandler<PendingStart>() {
            private final List<PendingStart> buffered = new ArrayList<>();

            @Override
            public void onEvent(PendingStart event, long sequence, boolean endOfBatch) throws Exception {
                buffered.add(event.copy());
                if (endOfBatch) {
                    flush();
                }
            }

            private void flush() {
                long toUncache = buffered.stream().mapToLong(p -> p.cachedUpTo).min().getAsLong();
                long numToAskFor = buffered.stream().mapToLong(p -> p.numberOfTransactions).sum();
                TimestampRange range = delegate.startTransactions(toUncache, numToAskFor);
                long start = range.getLower();
                for (PendingStart s : buffered) {
                    s.result.complete(TimestampRange.newBuilder()
                            .setImmutable(range.getImmutable())
                            .setLower(start)
                            .setUpper(start + s.numberOfTransactions)
                            .build());
                    start += s.numberOfTransactions;
                }
                buffered.clear();
            }
        });

        Disruptor<Unlock> unlockDisruptor = new Disruptor<>(Unlock::new, BUFFER_SIZE, threadFactory);
        unlockDisruptor.handleEventsWith(new EventHandler<Unlock>() {
            private final List<Long> toUnlock = new ArrayList<>();

            @Override
            public void onEvent(Unlock event, long sequence, boolean endOfBatch) throws Exception {
                toUnlock.addAll(event.startTimestamps);
                if (endOfBatch) {
                    flush();
                }
            }

            private void flush() {
                delegate.unlock(toUnlock);
                toUnlock.clear();
            }
        });

        return new AutoBatchingTransactionServiceClient(delegate, txnStartDisruptor.start(), unlockDisruptor.start());
    }

    private static class PendingStart {
        private long cachedUpTo;
        private long numberOfTransactions;
        private CompletableFuture<TimestampRange> result;

        PendingStart copy() {
            PendingStart copied = new PendingStart();
            copied.cachedUpTo = cachedUpTo;
            copied.numberOfTransactions = numberOfTransactions;
            copied.result = result;
            return copied;
        }
    }

    private static class Unlock {
        private List<Long> startTimestamps;
    }
}

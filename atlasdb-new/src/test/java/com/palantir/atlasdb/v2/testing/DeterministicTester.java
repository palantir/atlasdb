/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.v2.testing;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.v2.api.api.NewGetOperation;
import com.palantir.atlasdb.v2.api.api.NewIds;
import com.palantir.atlasdb.v2.api.api.NewIds.Cell;
import com.palantir.atlasdb.v2.api.api.NewIds.Column;
import com.palantir.atlasdb.v2.api.api.NewIds.Row;
import com.palantir.atlasdb.v2.api.api.NewIds.StoredValue;
import com.palantir.atlasdb.v2.api.api.NewIds.Table;
import com.palantir.atlasdb.v2.api.api.NewPutOperation;
import com.palantir.atlasdb.v2.api.api.NewTransaction;
import com.palantir.atlasdb.v2.api.api.ScanAttributes;
import com.palantir.atlasdb.v2.api.api.ScanFilter;
import com.palantir.atlasdb.v2.api.future.FutureChain;

import io.vavr.collection.LinkedHashSet;
import io.vavr.collection.Set;

public class DeterministicTester {
    private static final Table TABLE = NewIds.table("table.table1");

    private static final class SimplifiedTxn {
        private static final Column PRED = NewIds.column(new byte[1]);
        private static final Column SUCC = NewIds.column(new byte[2]);
        private static final Column SET = NewIds.column(new byte[3]);
        private final NewTransaction transaction;

        private SimplifiedTxn(NewTransaction transaction) {
            this.transaction = transaction;
        }

        public void setDebugging() {
            transaction.setIsDebugging();
        }

        public ListenableFuture<OptionalInt> getPred(int key) {
            Row row = NewIds.row(EncodingUtils.encodeSignedVarLong(key));
            return Futures.transform(transaction.get(new SimpleGetOperation(TABLE, NewIds.cell(row, PRED))),
                    value -> value.map(storedValue -> OptionalInt.of(
                            Ints.checkedCast(EncodingUtils.decodeSignedVarLong(storedValue.toByteArray())))).orElse(OptionalInt.empty()),
                    MoreExecutors.directExecutor());
        }

        public ListenableFuture<Integer> getPredOrElseThrow(int key) {
            return Futures.transform(getPred(key), OptionalInt::getAsInt, MoreExecutors.directExecutor());
        }

        public ListenableFuture<OptionalInt> getSucc(int key) {
            Row row = NewIds.row(EncodingUtils.encodeSignedVarLong(key));
            return Futures.transform(transaction.get(new SimpleGetOperation(TABLE, NewIds.cell(row, SUCC))),
                    value -> value.map(storedValue -> OptionalInt.of(
                            Ints.checkedCast(EncodingUtils.decodeSignedVarLong(storedValue.toByteArray())))).orElse(OptionalInt.empty()),
                    MoreExecutors.directExecutor());
        }

        public ListenableFuture<Integer> getSuccOrElseThrow(int key) {
            return Futures.transform(getSucc(key), OptionalInt::getAsInt, MoreExecutors.directExecutor());
        }

        public void addToSet(int element) {
            Row row = NewIds.row(EncodingUtils.encodeSignedVarLong(element));
            transaction.put(NewPutOperation.of(
                    TABLE,
                    NewIds.cell(row, SET),
                    Optional.of(NewIds.value(EncodingUtils.encodeSignedVarLong(1)))));
        }

        public void removeFromSet(int element) {
            Row row = NewIds.row(EncodingUtils.encodeSignedVarLong(element));
            transaction.put(NewPutOperation.of(
                    TABLE,
                    NewIds.cell(row, SET),
                    Optional.empty()));
        }

        public ListenableFuture<Boolean> setContains(int element) {
            Row row = NewIds.row(EncodingUtils.encodeSignedVarLong(element));
            return Futures.transform(transaction.get(new SimpleGetOperation(
                    TABLE, NewIds.cell(row, SET))), Optional::isPresent, MoreExecutors.directExecutor());
        }

        public void put(int key, int pred, int succ) {
            putPred(key, pred);
            putSucc(key, succ);
        }

        public void putPred(int key, int pred) {
            Row row = NewIds.row(EncodingUtils.encodeSignedVarLong(key));
            transaction.put(NewPutOperation.of(
                    TABLE,
                    NewIds.cell(row, PRED),
                    Optional.of(NewIds.value(EncodingUtils.encodeSignedVarLong(pred)))));
        }

        public void putSucc(int key, int succ) {
            Row row = NewIds.row(EncodingUtils.encodeSignedVarLong(key));
            transaction.put(NewPutOperation.of(
                    TABLE,
                    NewIds.cell(row, SUCC),
                    Optional.of(NewIds.value(EncodingUtils.encodeSignedVarLong(succ)))));
        }
    }

    private static TestTxnManager<SimplifiedTxn> newTxnManager() {
        return new TestTxnManager<>(SimplifiedTxn::new);
    }

    @Test
    public void testSimpleWrites() throws ExecutionException, InterruptedException {
        TestTxnManager<SimplifiedTxn> txnManager = newTxnManager();
        ListenableFuture<?> result = txnManager.callTransaction(txn -> {
            txn.put(1, 2, 3);
            return Futures.immediateFuture(null);
        });
        txnManager.executor().start();
        result.get();
    }

    @Test
    public void testWriteAndRead() throws ExecutionException, InterruptedException {
        TestTxnManager<SimplifiedTxn> txnManager = newTxnManager();
        ListenableFuture<?> writeResult = txnManager.callTransaction(txn -> {
            txn.put(1, 2, 3);
            return Futures.immediateFuture(null);
        });
        txnManager.executor().start();
        writeResult.get();
        ListenableFuture<OptionalInt> readResult = txnManager.callTransaction(txn -> txn.getSucc(1));
        txnManager.executor().start();
        assertThat(readResult.get().getAsInt()).isEqualTo(3);
    }

    @Test
    public void testDeterminism() {
        java.util.Set<SetMultimap<Long, Long>> traces = IntStream.range(0, 10)
                .mapToObj($ -> newTxnManager())
                .peek(txnManager -> {
                    try {
                        testPingPong(txnManager);
                    } catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                })
                .map(txnManager -> txnManager.executor().getTrace())
                .collect(toSet());
        assertThat(traces).hasSize(1);
    }

    @Test
    public void testPingPong() throws ExecutionException, InterruptedException {
        testPingPong(newTxnManager());
    }

    private static void testPingPong(
            TestTxnManager<SimplifiedTxn> txnManager) throws ExecutionException, InterruptedException {
        int universe = 4;
        ListenableFuture<?> state = txnManager.runTransaction(txn -> {
            txn.addToSet(0);
            txn.addToSet(1);
        });
        ListenableFuture<?> allSwaps = Futures.allAsList(IntStream.range(0, 16)
                .mapToObj($ -> Futures.whenAllSucceed(state)
                        .callAsync(() -> maybeReplaceSetElement(txnManager, universe, 10_000),
                                MoreExecutors.directExecutor()))
                .collect(toList()));
        ListenableFuture<java.util.Set<Integer>> stateAfterwards =
                Futures.whenAllSucceed(allSwaps).callAsync(
                        () -> getSet(txnManager, universe),
                        MoreExecutors.directExecutor());
        txnManager.executor().start();
        state.get();
        allSwaps.get();
        assertThat(stateAfterwards.get()).hasSize(2);
    }

    private static ListenableFuture<java.util.Set<Integer>> getSet(
            TestTxnManager<SimplifiedTxn> txnManager, int universeSize) {
        return txnManager.callTransaction(txn -> {
            List<Integer> range = IntStream.range(0, universeSize).boxed().collect(toList());
            ListenableFuture<List<Boolean>> membershipsListenableFuture = Futures.allAsList(Lists.transform(range, txn::setContains));
            return Futures.transform(membershipsListenableFuture, memberships -> {
                java.util.Set<Integer> result = new HashSet<>();
                for (int i = 0; i < range.size(); i++) {
                    if (memberships.get(i)) {
                        result.add(i);
                    }
                }
                return result;
            }, txnManager.executor().nowScheduler());
        });
    }

    private static ListenableFuture<?> maybeReplaceSetElement(
            TestTxnManager<SimplifiedTxn> txnManager, int universeSize, int numTimes) {
        return FutureChain.start(txnManager.executor().soonScheduler(), 0)
                .whileTrue(i -> i < numTimes,
                        chain -> chain.then(i -> txnManager.callTransaction(
                                txn -> maybeMoveOne(txnManager, txn, universeSize, i)))
                                .alterState(i -> i + 1))
                .done();
    }

    private static ListenableFuture<?> maybeMoveOne(
            TestTxnManager txnManager, SimplifiedTxn txn, int universeSize, int round) {
        int a = txnManager.executor().randomInt(universeSize);
        int b = txnManager.executor().randomInt(universeSize);

        if (a == b) {
            return Futures.immediateFuture(null);
        }

        ListenableFuture<Boolean> aExists = txn.setContains(a);
        ListenableFuture<Boolean> bExists = txn.setContains(b);
        return Futures.whenAllSucceed(aExists, bExists)
                .call(() -> {
                    boolean isA = aExists.get();
                    boolean isB = bExists.get();
                    if (isA && isB) {
                        int newElement = txnManager.executor().randomInt(universeSize);
                        if (newElement != b) {
                            txn.removeFromSet(a);
                            txn.addToSet(newElement);
                        }
                    }
                    return null;
                }, txnManager.executor().nowScheduler());
    }

    @Test
    public void testRingMutations() throws ExecutionException, InterruptedException {
        TestTxnManager<SimplifiedTxn> txnManager = newTxnManager();
        int ringLength = 1000;
        ListenableFuture<?> writes = txnManager.runTransaction(txn -> buildRing(txn, ringLength));
        txnManager.executor().start();
        writes.get();

        ListenableFuture<?> swappages = Futures.allAsList(IntStream.range(0, 16)
                .mapToObj($ -> swapElements(txnManager, ringLength, 10_000))
                .collect(toList()));
        txnManager.executor().start();
        swappages.get();

        ListenableFuture<Integer> actualRingLength = txnManager.callTransaction(
                txn -> ringLengthViaSucc(txnManager, txn));
        txnManager.executor().start();
        assertThat(actualRingLength.get()).isEqualTo(ringLength);
    }

    private ListenableFuture<?> swapElements(TestTxnManager<SimplifiedTxn> txnManager, int ringLength, int numTimes) {
        return FutureChain.start(txnManager.executor().soonScheduler(), 0)
                .whileTrue(i -> i < numTimes,
                        chain -> chain.then(i -> txnManager.callTransaction(
                                txn -> swapElements(txnManager, txn, ringLength, i)))
                                .alterState(i -> i + 1))
                .done();
    }

    private ListenableFuture<?> swapElements(
            TestTxnManager txnManager, SimplifiedTxn txn, int ringLength, int iteration) {
        int a = txnManager.executor().randomInt(ringLength);
        int b = txnManager.executor().randomInt(ringLength);
        if (a == b) {
            return Futures.immediateFuture(null);
        }
        ListenableFuture<Integer> aPred = txn.getPredOrElseThrow(a);
        ListenableFuture<Integer> bPred = txn.getPredOrElseThrow(b);
        ListenableFuture<Integer> aSucc = txn.getSuccOrElseThrow(a);
        ListenableFuture<Integer> bSucc = txn.getSuccOrElseThrow(b);
        return Futures.whenAllSucceed(aPred, bPred, aSucc, bSucc).call(() -> {
            int ap = aPred.get();
            int bp = bPred.get();
            int as = aSucc.get();
            int bs = bSucc.get();

//            System.out.println(a + " -> [" + ap + "," + as + "], " + b + " -> [" + bp + "," + bs + "]");
            if (ap == b || as == b || bp == a || bs == a) {
                return null;
            }
            txn.put(a, bp, bs);
            txn.putPred(bs, a);
            txn.putSucc(bp, a);
            txn.put(b, ap, as);
            txn.putPred(as, b);
            txn.putSucc(ap, b);
            return null;
        }, txnManager.executor().nowScheduler());
    }

    private void buildRing(SimplifiedTxn txn, int ringLength) {
        for (int i = 0; i < ringLength; i++) {
            txn.put(i, (ringLength + i - 1) % ringLength, (i + 1) % ringLength);
        }
    }

    private ListenableFuture<Integer> ringLengthViaSucc(TestTxnManager txnManager, SimplifiedTxn txn) {

        Predicate<LinkedHashSet<Integer>> setChanged = new Predicate<LinkedHashSet<Integer>>() {
            LinkedHashSet<Integer> last = LinkedHashSet.empty();

            @Override
            public boolean test(LinkedHashSet<Integer> integers) {
                if (last.equals(integers)) {
                    return false;
                }
                last = integers;
                return true;
            }
        };
        return FutureChain.start(txnManager.executor().nowScheduler(), LinkedHashSet.of(0))
                .whileTrue(setChanged,
                        seen -> seen.then(s -> txn.getSuccOrElseThrow(s.last()), LinkedHashSet::add))
                .alterState(Set::size)
                .done();
    }

    private static final class SimpleGetOperation implements NewGetOperation<Optional<StoredValue>> {
        private final Table table;
        private final Cell cell;

        private SimpleGetOperation(Table table, Cell cell) {
            this.table = table;
            this.cell = cell;
        }

        @Override
        public Table table() {
            return table;
        }

        @Override
        public ScanAttributes attributes() {
            return new ScanAttributes();
        }

        @Override
        public ScanFilter scanFilter() {
            return ScanFilter.forCell(cell);
        }

        @Override
        public ResultBuilder<Optional<StoredValue>> newResultBuilder() {
            return new ResultBuilder<Optional<StoredValue>>() {
                Optional<StoredValue> result = Optional.empty();

                @Override
                public boolean isDone() {
                    return false;
                }

                @Override
                public ResultBuilder<Optional<StoredValue>> add(Table table, Cell cell, StoredValue value) {
                    result = Optional.of(value);
                    return this;
                }

                @Override
                public Optional<StoredValue> build() {
                    return result;
                }
            };
        }
    }
}

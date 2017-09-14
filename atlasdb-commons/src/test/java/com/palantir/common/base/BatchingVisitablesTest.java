/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.common.base;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import javax.annotation.Nullable;

import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.UnmodifiableIterator;
import com.palantir.util.Mutable;
import com.palantir.util.Mutables;
import com.palantir.util.Pair;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

@SuppressWarnings("Guava") // BatchingVisitables uses Guava.
public class BatchingVisitablesTest {
    @Test
    public void testGetFirstPage() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L));

        TokenBackedBasicResultsPage<Long, Long> page = BatchingVisitables.getFirstPage(visitor, 0);
        assertEquals("anonymous assert 6BC9FC", 0, page.getResults().size());
        assertEquals("anonymous assert 7EBA34", true, page.moreResultsAvailable());

        page = BatchingVisitables.getFirstPage(visitor, 1);
        assertEquals("anonymous assert 1B3BD6", 1, page.getResults().size());
        assertEquals("anonymous assert BDAB68", Lists.newArrayList(0L), page.getResults());
        assertEquals("anonymous assert F4E06A", true, page.moreResultsAvailable());

        page = BatchingVisitables.getFirstPage(visitor, 3);
        assertEquals("anonymous assert 87CB3C", 3, page.getResults().size());
        assertEquals("anonymous assert 0AB14A", Lists.newArrayList(0L, 1L, 2L), page.getResults());
        assertEquals("anonymous assert 178C51", true, page.moreResultsAvailable());

        page = BatchingVisitables.getFirstPage(visitor, 4);
        assertEquals("anonymous assert 6215D2", 4, page.getResults().size());
        assertEquals("anonymous assert C1D8C0", Lists.newArrayList(0L, 1L, 2L, 3L), page.getResults());
        assertEquals("anonymous assert 91F5FF", false, page.moreResultsAvailable());

        page = BatchingVisitables.getFirstPage(visitor, 7);
        assertEquals("anonymous assert 77AABC", 4, page.getResults().size());
        assertEquals("anonymous assert E250DA", Lists.newArrayList(0L, 1L, 2L, 3L), page.getResults());
        assertEquals("anonymous assert 516AA9", false, page.moreResultsAvailable());

        try {
            BatchingVisitables.getFirstPage(visitor, -1);
            fail("Should not allow visiting -1 elements.");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testMinMax() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L));

        assertEquals("anonymous assert 3C39F5", 0L, (long) BatchingVisitables.getMin(visitor));
        assertEquals("anonymous assert CC2B13", 3L, (long) BatchingVisitables.getMax(visitor));

        BatchingVisitable<Pair<Long, Long>> pairedVisitable = ListVisitor.create(Lists.newArrayList(
                Pair.create(0L, 0L),
                Pair.create(1L, 0L),
                Pair.create(0L, 1L),
                Pair.create(1L, 1L)));

        Ordering<Pair<Long, Long>> ordering = Pair.compareLhSide();
        assertEquals("anonymous assert C849C7", Pair.create(0L, 0L),
                BatchingVisitables.getMin(pairedVisitable, ordering, null));
        assertEquals("anonymous assert A7358D", Pair.create(1L, 0L),
                BatchingVisitables.getMax(pairedVisitable, ordering, null));
    }

    @Test
    public void testEmpty() {
        assertTrue("anonymous assert 08BE89",
                BatchingVisitables.isEmpty(BatchingVisitables.emptyBatchingVisitable()));
        assertEquals("anonymous assert 3E0F41", 0,
                BatchingVisitables.count(BatchingVisitables.emptyBatchingVisitable()));

        BatchingVisitable<Long> bv = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L));
        assertFalse("anonymous assert DDDE16", BatchingVisitables.isEmpty(bv));
        assertEquals("anonymous assert 1832D8", 4, BatchingVisitables.count(bv));
        assertTrue("anonymous assert C6A211",
                BatchingVisitables.emptyBatchingVisitable().batchAccept(1, AbortingVisitors.alwaysFalse()));
    }

    @Test
    public void testPageSize() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L));
        visitor.batchAccept(5, item -> {
            assertEquals("anonymous assert 8A150B", 5, item.size());
            return false;
        });
        visitor.batchAccept(1, item -> {
            assertEquals("anonymous assert 49A3AF", 1, item.size());
            return false;
        });
        visitor.batchAccept(2, item -> {
            assertEquals("anonymous assert B07B27", 2, item.size());
            return true;
        });

        visitor.batchAccept(4, item -> {
            Preconditions.checkState(item.size() == 4);
            return false;
        });

        visitor.batchAccept(4, item -> {
            Preconditions.checkState(item.size() == 4 || item.size() == 2);
            return true;
        });

        visitor.batchAccept(20, item -> {
            assertEquals("anonymous assert 0757C9", 6, item.size());
            return true;
        });
    }

    @Test
    public void testBatchHints() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L));
        Function<List<Long>, List<String>> trans = (input) -> {
            assertEquals("anonymous assert 110D40", 2, input.size());
            return Lists.transform(input, Functions.toStringFunction());
        };
        BatchingVisitableView<String> visitable = BatchingVisitableView.of(visitor).transformBatch(trans)
                .hintBatchSize(2);
        final Mutable<Boolean> hasTripped = Mutables.newMutable();
        visitable.batchAccept(10000, item -> {
            hasTripped.set(true);
            assertEquals("anonymous assert F13DE1", 8, item.size());
            return false;
        });
        assertTrue("anonymous assert 323FF5", hasTripped.get());
    }

    @Test
    public void testBatchWrap() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L));
        final Mutable<Boolean> hasTripped = Mutables.newMutable();
        AbortingVisitor<List<Object>, RuntimeException> bv = item -> {
            hasTripped.set(true);
            assertEquals("anonymous assert 626F1C", 8, item.size());
            return false;
        };
        AbortingVisitor<List<Long>, RuntimeException> wrap = AbortingVisitors.wrapBatching(bv);
        BatchingVisitableView.of(visitor).batchAccept(1000, wrap);

        assertTrue("anonymous assert 755B51", hasTripped.get());
    }

    @Test
    public void testBatchWrap2() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L));
        final Mutable<Boolean> hasTripped = Mutables.newMutable();
        AbortingVisitor<List<? extends Long>, RuntimeException> bv = item -> {
            hasTripped.set(true);
            assertEquals("anonymous assert 2BC193", 8, item.size());
            return false;
        };
        AbortingVisitor<List<Long>, RuntimeException> wrap = AbortingVisitors.wrapBatching(bv);
        BatchingVisitableView.of(visitor).batchAccept(1000, wrap);

        assertTrue("anonymous assert 380CE5", hasTripped.get());
    }

    @Test
    public void testUnique() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(
                0L, 1L, 1L, 2L, 2L, 2L, 3L, 4L, 5L, 5L, 6L, 7L, 7L));
        BatchingVisitableView<Long> bv = BatchingVisitableView.of(visitor);
        assertEquals("anonymous assert 139904", ImmutableList.of(0L, 1L, 2L, 3L), bv.unique().limit(4).immutableCopy());
    }

    @Test
    public void testLimit() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L));
        BatchingVisitableView<Long> limited = BatchingVisitableView.of(visitor).limit(3);
        ImmutableList<Long> copy = limited.immutableCopy();
        assertEquals("anonymous assert 939C77", ImmutableList.of(0L, 1L, 2L), copy);
        copy = BatchingVisitableView.of(visitor).limit(3).immutableCopy();
        assertEquals("anonymous assert 67B112", ImmutableList.of(0L, 1L, 2L), copy);
        copy = limited.immutableCopy();
        assertEquals("anonymous assert BE4BE6", ImmutableList.of(0L, 1L, 2L), copy);
        assertFalse("anonymous assert 38D61C", limited.batchAccept(1, AbortingVisitors.alwaysFalse()));
        assertTrue("anonymous assert 70A3FC", limited.batchAccept(1, AbortingVisitors.alwaysTrue()));

        assertTrue("anonymous assert 33212B", limited.batchAccept(2, AbortingVisitors.alwaysTrue()));
        assertTrue("anonymous assert 8EF750", limited.batchAccept(3, AbortingVisitors.alwaysTrue()));

        CountingVisitor<Long> cv = new CountingVisitor<>();
        assertTrue("anonymous assert F935C9", limited.batchAccept(2, AbortingVisitors.batching(cv)));
        assertEquals("anonymous assert 24BD1B", 3, cv.count);
        assertTrue("anonymous assert 15643D", limited.batchAccept(3, AbortingVisitors.batching(cv)));
        assertEquals("anonymous assert 97E616", 6, cv.count);
        assertTrue("anonymous assert 93B081", limited.batchAccept(4, AbortingVisitors.batching(cv)));
        assertEquals("anonymous assert 53317E", 9, cv.count);
        assertTrue("anonymous assert C44EC3", limited.skip(3).batchAccept(1, AbortingVisitors.alwaysFalse()));

        LimitVisitor<Long> lv = new LimitVisitor<>(3);
        assertFalse("anonymous assert 94391F", limited.batchAccept(1, AbortingVisitors.batching(lv)));
        lv = new LimitVisitor<>(4);
        assertTrue("anonymous assert 1830FC", limited.batchAccept(1, AbortingVisitors.batching(lv)));
        lv = new LimitVisitor<>(2);
        assertFalse("anonymous assert 52C831", limited.batchAccept(1, AbortingVisitors.batching(lv)));

        assertFalse("anonymous assert 21BD70", limited.hintBatchSize(10)
                .batchAccept(1, AbortingVisitors.alwaysFalse()));
        assertTrue("anonymous assert 530244", limited.hintBatchSize(10).skip(3)
                .batchAccept(1, AbortingVisitors.alwaysFalse()));

        assertEquals("anonymous assert 22261C", 2, limited.limit(2).limit(4).size());
        assertEquals("anonymous assert 860268", 2, limited.limit(4).limit(2).size());
        assertEquals("anonymous assert 56B69B", 3, limited.limit(4).size());
    }

    @Test
    public void testConcat() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L));
        BatchingVisitableView<Long> limited = BatchingVisitableView.of(visitor).limit(3);
        BatchingVisitableView<Long> concat = BatchingVisitables.concat(limited, visitor);
        assertTrue("anonymous assert CED0ED", concat.batchAccept(2, AbortingVisitors
                .batching(AbortingVisitors.alwaysTrue())));
    }

    static class CountingVisitor<T> implements AbortingVisitor<T, RuntimeException> {
        long count = 0;

        @Override
        public boolean visit(T item) throws RuntimeException {
            count++;
            return true;
        }
    }

    static class LimitVisitor<T> implements AbortingVisitor<T, RuntimeException> {
        final long limit;
        long count = 0;

        LimitVisitor(long limit) {
            this.limit = limit;
        }

        @Override
        public boolean visit(T item) throws RuntimeException {
            count++;
            return count < limit;
        }
    }

    @Test
    public void testAny() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L));
        BatchingVisitableView<Long> bv = BatchingVisitableView.of(visitor);
        assertTrue("anonymous assert 447C52", bv.any(Predicates.alwaysTrue()));
        assertTrue("anonymous assert 6A41BA", bv.all(Predicates.alwaysTrue()));

        assertFalse("anonymous assert 8F527C", bv.any(Predicates.alwaysFalse()));
        assertFalse("anonymous assert 28FD17", bv.all(Predicates.alwaysFalse()));
    }

    @Test
    public void testAll() {
        BatchingVisitable<Long> visitor = BatchingVisitables.emptyBatchingVisitable();
        BatchingVisitableView<Long> bv = BatchingVisitableView.of(visitor);
        assertFalse("anonymous assert 94F1C3", bv.any(Predicates.alwaysTrue()));
        assertTrue("anonymous assert ED0ED7", bv.all(Predicates.alwaysTrue()));

        assertFalse("anonymous assert FDEDF7", bv.any(Predicates.alwaysFalse()));
        assertTrue("anonymous assert 01080C", bv.all(Predicates.alwaysFalse()));
    }

    @Test
    public void testHintPageSize() {
        InfiniteVisitable infinite = new InfiniteVisitable();
        BatchingVisitableView<Long> bv = BatchingVisitableView.of(infinite);
        long first = bv.filter(new TakeEvery<>(100)).getFirst();
        assertEquals("anonymous assert A16A34", 99L, first);
        assertEquals("anonymous assert 55EE6D", 100L, infinite.count);

        first = bv.filter(new TakeEvery<>(100)).hintBatchSize(100).getFirst();
        assertEquals("anonymous assert 709532", 199L, first);
        assertEquals("anonymous assert D4A406", 200L, infinite.count);
    }

    static class InfiniteVisitable extends AbstractBatchingVisitable<Long> {
        long count = 0;

        @Override
        protected <K extends Exception> void batchAcceptSizeHint(int batchSizeHint,
                ConsistentVisitor<Long, K> bv)
                throws K {
            while (bv.visitOne(count++)) { /* */ }
        }
    }

    static class TakeEvery<T> implements Predicate<T> {
        final long numToSkip;
        long count = 0;

        TakeEvery(long numToSkip) {
            this.numToSkip = numToSkip;
        }

        @Override
        public boolean apply(@Nullable T input) {
            return ++count % numToSkip == 0;
        }
    }

    @Test
    public void testSkip() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L));
        ImmutableList<Long> copy = BatchingVisitableView.of(visitor).skip(5).immutableCopy();
        assertEquals("anonymous assert 82C94E", ImmutableList.of(5L, 6L, 7L), copy);
    }

    static class ListVisitor<T> extends AbstractBatchingVisitable<T> {
        private final List<T> listToVisit;

        ListVisitor(List<T> list) {
            this.listToVisit = list;
        }

        public static <T> BatchingVisitable<T> create(List<T> list) {
            return new ListVisitor<>(list);
        }

        @Override
        protected <K extends Exception> void batchAcceptSizeHint(
                int batchSize,
                ConsistentVisitor<T, K> bv) throws K {
            int actualBatchSize = Math.max(2, batchSize / 2);

            for (List<T> subList : Lists.partition(listToVisit, actualBatchSize)) {
                boolean moreToVisit = bv.visit(subList);

                if (!moreToVisit) {
                    return;
                }
            }
        }
    }

    @Test
    public void testVisitWhile() {
        List<Long> longList = Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        BatchingVisitable<Long> visitable = ListVisitor.create(longList);
        BatchingVisitableView<Long> view = BatchingVisitables.visitWhile(
                BatchingVisitableView.of(visitable), (input) -> input.longValue() < 5L);
        assertEquals("anonymous assert 7B93D7", 5L, view.size());
        assertEquals("anonymous assert 5FCD36", 0L, view.getFirst().longValue());
        assertEquals("anonymous assert 0280B1", 4L, view.getLast().longValue());
        assertTrue("anonymous assert 03417A", view.immutableCopy().containsAll(ImmutableSet.of(0L, 1L, 2L, 3L, 4L)));

        visitable = ListVisitor.create(Lists.reverse(longList));
        view = BatchingVisitables.visitWhile(
                BatchingVisitableView.of(visitable), (input) -> input.longValue() >= 5L);
        assertEquals("anonymous assert FFBC60", 5L, view.size());
        assertEquals("anonymous assert E46C52", 9L, view.getFirst().longValue());
        assertEquals("anonymous assert 8D6CA6", 5L, view.getLast().longValue());
        assertTrue("anonymous assert 165D8C", view.immutableCopy().containsAll(ImmutableSet.of(5L, 6L, 7L, 8L, 9L)));
    }

    @Test
    public void testFlatten() {
        List<String> firstChars = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            firstChars.add("" + (char) ('a' + i));
        }
        BatchingVisitable<String> outerVisitable = BatchingVisitableFromIterable.create(firstChars);
        BatchingVisitableView<BatchingVisitable<String>> bv = BatchingVisitables.transform(outerVisitable,
                (prefix) -> {
                    List<String> innerChars = Lists.newArrayList();
                    for (int i = 0; i < 10; i++) {
                        innerChars.add(prefix + (char) ('0' + i));
                    }
                    return BatchingVisitableFromIterable.create(innerChars);
                });
        UnmodifiableIterator<String> iter = BatchingVisitables.flatten(7, bv).immutableCopy().iterator();
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                assertEquals("anonymous assert E5D518", "" + (char) ('a' + i) + (char) ('0' + j), iter.next());
            }
        }
    }
}

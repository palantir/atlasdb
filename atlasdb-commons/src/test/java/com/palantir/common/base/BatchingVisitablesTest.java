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
package com.palantir.common.base;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

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
import java.util.List;
import javax.annotation.Nullable;
import org.junit.Test;

@SuppressWarnings("Guava") // BatchingVisitables uses Guava.
public class BatchingVisitablesTest {
    @Test
    public void testGetFirstPage() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L));

        TokenBackedBasicResultsPage<Long, Long> page = BatchingVisitables.getFirstPage(visitor, 0);
        assertEquals("page results had wrong size!", 0, page.getResults().size());
        assertEquals("page.moreResultsAvailable was wrong", true, page.moreResultsAvailable());

        page = BatchingVisitables.getFirstPage(visitor, 1);
        assertEquals("page results had wrong size!", 1, page.getResults().size());
        assertEquals("page.getResults was wrong", Lists.newArrayList(0L), page.getResults());
        assertEquals("page.moreResultsAvailable was wrong", true, page.moreResultsAvailable());

        page = BatchingVisitables.getFirstPage(visitor, 3);
        assertEquals("page results had wrong size!", 3, page.getResults().size());
        assertEquals("page.getResults was wrong", Lists.newArrayList(0L, 1L, 2L), page.getResults());
        assertEquals("page.moreResultsAvailable was wrong", true, page.moreResultsAvailable());

        page = BatchingVisitables.getFirstPage(visitor, 4);
        assertEquals("page results had wrong size!", 4, page.getResults().size());
        assertEquals("page.getResults was wrong", Lists.newArrayList(0L, 1L, 2L, 3L), page.getResults());
        assertEquals("page.moreResultsAvailable was wrong", false, page.moreResultsAvailable());

        page = BatchingVisitables.getFirstPage(visitor, 7);
        assertEquals("page results had wrong size!", 4, page.getResults().size());
        assertEquals("page.getResults was wrong", Lists.newArrayList(0L, 1L, 2L, 3L), page.getResults());
        assertEquals("page.moreResultsAvailable was wrong", false, page.moreResultsAvailable());

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

        assertEquals("BatchingVisitables.getMin was wrong", 0L, (long) BatchingVisitables.getMin(visitor));
        assertEquals("BatchingVisitables.getMax was wrong", 3L, (long) BatchingVisitables.getMax(visitor));

        BatchingVisitable<Pair<Long, Long>> pairedVisitable = ListVisitor.create(Lists.newArrayList(
                Pair.create(0L, 0L),
                Pair.create(1L, 0L),
                Pair.create(0L, 1L),
                Pair.create(1L, 1L)));

        Ordering<Pair<Long, Long>> ordering = Pair.compareLhSide();
        assertEquals("BatchingVisitables.getMin was wrong", Pair.create(0L, 0L),
                BatchingVisitables.getMin(pairedVisitable, ordering, null));
        assertEquals("BatchingVisitables.getMax was wrong", Pair.create(1L, 0L),
                BatchingVisitables.getMax(pairedVisitable, ordering, null));
    }

    @Test
    public void testEmpty() {
        assertTrue("empty batching visitable should be empty!",
                BatchingVisitables.isEmpty(BatchingVisitables.emptyBatchingVisitable()));
        assertEquals("empty batching visitable should be empty!", 0,
                BatchingVisitables.count(BatchingVisitables.emptyBatchingVisitable()));

        BatchingVisitable<Long> bv = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L));
        assertFalse("non-empty batching visitable should not be empty!", BatchingVisitables.isEmpty(bv));
        assertEquals("visitable had wrong size", 4, BatchingVisitables.count(bv));
        assertTrue("empty visitable should always be true, even when told to visit an always-false place",
                BatchingVisitables.emptyBatchingVisitable().batchAccept(1, AbortingVisitors.alwaysFalse()));
    }

    @Test
    public void testPageSize() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L));
        visitor.batchAccept(5, item -> {
            assertEquals("batched item had wrong size", 5, item.size());
            return false;
        });
        visitor.batchAccept(1, item -> {
            assertEquals("batched item had wrong size", 1, item.size());
            return false;
        });
        visitor.batchAccept(2, item -> {
            assertEquals("batched item had wrong size", 2, item.size());
            return true;
        });

        visitor.batchAccept(4, item -> {
            Preconditions.checkState(item.size() == 4, "batched item had wrong size");
            return false;
        });

        visitor.batchAccept(4, item -> {
            Preconditions.checkState(item.size() == 4 || item.size() == 2, "batched item had wrong size");
            return true;
        });

        visitor.batchAccept(20, item -> {
            assertEquals("batched item had wrong size", 6, item.size());
            return true;
        });
    }

    @Test
    public void testBatchHints() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L));
        Function<List<Long>, List<String>> trans = (input) -> {
            assertEquals("batched item had wrong size", 2, input.size());
            return Lists.transform(input, Functions.toStringFunction());
        };
        BatchingVisitableView<String> visitable = BatchingVisitableView.of(visitor).transformBatch(trans)
                .hintBatchSize(2);
        final Mutable<Boolean> hasTripped = Mutables.newMutable();
        visitable.batchAccept(10000, item -> {
            hasTripped.set(true);
            assertEquals("batched item had wrong size", 8, item.size());
            return false;
        });
        assertTrue("should have been tripped!", hasTripped.get());
    }

    @Test
    public void testImmutableCopyRespectsCustomBatchSize() throws Exception {
        AbstractBatchingVisitable bv = mock(AbstractBatchingVisitable.class);

        BatchingVisitableView bvv = BatchingVisitableView.of(bv);
        bvv.hintBatchSize(2).immutableCopy();

        verify(bv).batchAcceptSizeHint(eq(2), any());
    }

    @Test
    public void testImmutableCopyWithDefaultBatchSize() throws Exception {
        AbstractBatchingVisitable bv = mock(AbstractBatchingVisitable.class);

        BatchingVisitableView bvv = BatchingVisitableView.of(bv);
        bvv.immutableCopy();

        verify(bv).batchAcceptSizeHint(eq(BatchingVisitables.DEFAULT_BATCH_SIZE), any());
    }

    @Test
    public void testBatchWrap() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L));
        final Mutable<Boolean> hasTripped = Mutables.newMutable();
        AbortingVisitor<List<Object>, RuntimeException> bv = item -> {
            hasTripped.set(true);
            assertEquals("batched item had wrong size", 8, item.size());
            return false;
        };
        AbortingVisitor<List<Long>, RuntimeException> wrap = AbortingVisitors.wrapBatching(bv);
        BatchingVisitableView.of(visitor).batchAccept(1000, wrap);

        assertTrue("should have been tripped!", hasTripped.get());
    }

    @Test
    public void testBatchWrap2() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L));
        final Mutable<Boolean> hasTripped = Mutables.newMutable();
        AbortingVisitor<List<? extends Long>, RuntimeException> bv = item -> {
            hasTripped.set(true);
            assertEquals("batched item had wrong size", 8, item.size());
            return false;
        };
        AbortingVisitor<List<Long>, RuntimeException> wrap = AbortingVisitors.wrapBatching(bv);
        BatchingVisitableView.of(visitor).batchAccept(1000, wrap);

        assertTrue("should have been tripped!", hasTripped.get());
    }

    @Test
    public void testUnique() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(
                0L, 1L, 1L, 2L, 2L, 2L, 3L, 4L, 5L, 5L, 6L, 7L, 7L));
        BatchingVisitableView<Long> bv = BatchingVisitableView.of(visitor);
        assertEquals("unexpected result for unique view",
                ImmutableList.of(0L, 1L, 2L, 3L),
                bv.unique().limit(4).immutableCopy());
    }

    @Test
    public void testLimit() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L));
        BatchingVisitableView<Long> limited = BatchingVisitableView.of(visitor).limit(3);
        ImmutableList<Long> copy = limited.immutableCopy();
        assertEquals("limit produced unexpected result", ImmutableList.of(0L, 1L, 2L), copy);

        assertFalse("alwaysFalse should be false", limited.batchAccept(1, AbortingVisitors.alwaysFalse()));
        assertTrue("alwaysTrue should be true", limited.batchAccept(1, AbortingVisitors.alwaysTrue()));

        assertTrue("alwaysTrue should be true twice", limited.batchAccept(2, AbortingVisitors.alwaysTrue()));
        assertTrue("alwaysTrue should be true thrice", limited.batchAccept(3, AbortingVisitors.alwaysTrue()));

        CountingVisitor<Long> cv = new CountingVisitor<>();
        assertTrue("batchAccept should be true", limited.batchAccept(2, AbortingVisitors.batching(cv)));
        assertEquals("CountingVisitor had wrong count", 3, cv.count);
        assertTrue("batchAccept should be true", limited.batchAccept(3, AbortingVisitors.batching(cv)));
        assertEquals("CountingVisitor had wrong count", 6, cv.count);
        assertTrue("batchAccept should be true", limited.batchAccept(4, AbortingVisitors.batching(cv)));
        assertEquals("CountingVisitor had wrong count", 9, cv.count);

        assertTrue("batchAccept should be trivially true after everything was skipped",
                limited.skip(3).batchAccept(1, AbortingVisitors.alwaysFalse()));

        LimitVisitor<Long> lv = new LimitVisitor<>(3);
        assertFalse("batchAccept should be false", limited.batchAccept(1, AbortingVisitors.batching(lv)));
        lv = new LimitVisitor<>(4);
        assertTrue("batchAccept should be true", limited.batchAccept(1, AbortingVisitors.batching(lv)));
        lv = new LimitVisitor<>(2);
        assertFalse("batchAccept should be false", limited.batchAccept(1, AbortingVisitors.batching(lv)));

        assertFalse("batchAccept should be false", limited.hintBatchSize(10)
                .batchAccept(1, AbortingVisitors.alwaysFalse()));
        assertTrue("batchAccept should be trivially true after everything was skipped", limited.hintBatchSize(10).skip(3)
                .batchAccept(1, AbortingVisitors.alwaysFalse()));

        assertEquals("smaller limits should precede larger limits", 2, limited.limit(2).limit(4).size());
        assertEquals("smaller limits should precede larger limits", 2, limited.limit(4).limit(2).size());
        assertEquals("limited size shouldn't be greater than the original size!", 3, limited.limit(4).size());
    }

    @Test
    public void testConcat() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L));
        BatchingVisitableView<Long> limited = BatchingVisitableView.of(visitor).limit(3);
        BatchingVisitableView<Long> concat = BatchingVisitables.concat(limited, visitor);
        assertTrue("concatenated batchAccept should be true", concat.batchAccept(2, AbortingVisitors
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
    public void testAnyAndAllForNonEmptyLists() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L));
        BatchingVisitableView<Long> bv = BatchingVisitableView.of(visitor);
        assertTrue("any(true) should be true", bv.any(Predicates.alwaysTrue()));
        assertTrue("all(true) should be true", bv.all(Predicates.alwaysTrue()));

        assertFalse("any(false) should be false", bv.any(Predicates.alwaysFalse()));
        assertFalse("all(false) should be false", bv.all(Predicates.alwaysFalse()));
    }

    @Test
    public void testAnyAndAllForEmptyLists() {
        BatchingVisitable<Long> visitor = BatchingVisitables.emptyBatchingVisitable();
        BatchingVisitableView<Long> bv = BatchingVisitableView.of(visitor);
        assertFalse("any(empty-set-of-trues) should be false", bv.any(Predicates.alwaysTrue()));
        assertTrue("all(empty-set-of-trues) should be true", bv.all(Predicates.alwaysTrue()));

        assertFalse("any(empty-set-of-falses) should be false", bv.any(Predicates.alwaysFalse()));
        assertTrue("all(empty-set-of-falses) should be true", bv.all(Predicates.alwaysFalse()));
    }

    @Test
    public void testHintPageSize() {
        InfiniteVisitable infinite = new InfiniteVisitable();
        BatchingVisitableView<Long> bv = BatchingVisitableView.of(infinite);
        long first = bv.filter(new TakeEvery<>(100)).getFirst();
        assertEquals("first element returned was wrong", 99L, first);
        assertEquals("count of InfiniteVisitable didn't match", 100L, infinite.count);

        first = bv.filter(new TakeEvery<>(100)).hintBatchSize(100).getFirst();
        assertEquals("first element returned was wrong", 199L, first);
        assertEquals("count of InfiniteVisitable didn't match", 200L, infinite.count);
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
        assertEquals("unexpected list remnants after skipping", ImmutableList.of(5L, 6L, 7L), copy);
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
        assertEquals("visitWhile visited the wrong number of elements", 5L, view.size());
        assertEquals("visitWhile visited the wrong element first", 0L, view.getFirst().longValue());
        assertEquals("visitWhile visited the wrong element last", 4L, view.getLast().longValue());
        assertTrue("visitWhile visited the wrong elements",
                view.immutableCopy().containsAll(ImmutableSet.of(0L, 1L, 2L, 3L, 4L)));

        visitable = ListVisitor.create(Lists.reverse(longList));
        view = BatchingVisitables.visitWhile(
                BatchingVisitableView.of(visitable), (input) -> input.longValue() >= 5L);
        assertEquals("visitWhile visited the wrong number of elements", 5L, view.size());
        assertEquals("visitWhile visited the wrong element first", 9L, view.getFirst().longValue());
        assertEquals("visitWhile visited the wrong element last", 5L, view.getLast().longValue());
        assertTrue("visitWhile visited the wrong elements",
                view.immutableCopy().containsAll(ImmutableSet.of(5L, 6L, 7L, 8L, 9L)));
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
                assertEquals("unexpected flattened result", "" + (char) ('a' + i) + (char) ('0' + j), iter.next());
            }
        }
    }
}

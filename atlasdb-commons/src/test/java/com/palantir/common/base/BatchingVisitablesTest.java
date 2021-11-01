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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("Guava") // BatchingVisitables uses Guava.
public class BatchingVisitablesTest {
    @Mock
    private AbstractBatchingVisitable<Void> bv;

    @Test
    public void testGetFirstPage() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L));

        TokenBackedBasicResultsPage<Long, Long> page = BatchingVisitables.getFirstPage(visitor, 0);
        assertThat(page.getResults())
                .describedAs("page results had wrong size!")
                .isEmpty();
        assertThat(page.moreResultsAvailable())
                .describedAs("page.moreResultsAvailable was wrong")
                .isTrue();

        page = BatchingVisitables.getFirstPage(visitor, 1);
        assertThat(page.getResults())
                .describedAs("page results had wrong size!")
                .hasSize(1);
        assertThat(page.getResults()).describedAs("page.getResults was wrong").isEqualTo(Lists.newArrayList(0L));
        assertThat(page.moreResultsAvailable())
                .describedAs("page.moreResultsAvailable was wrong")
                .isTrue();

        page = BatchingVisitables.getFirstPage(visitor, 3);
        assertThat(page.getResults())
                .describedAs("page results had wrong size!")
                .hasSize(3);
        assertThat(page.getResults())
                .describedAs("page.getResults was wrong")
                .isEqualTo(Lists.newArrayList(0L, 1L, 2L));
        assertThat(page.moreResultsAvailable())
                .describedAs("page.moreResultsAvailable was wrong")
                .isTrue();

        page = BatchingVisitables.getFirstPage(visitor, 4);
        assertThat(page.getResults())
                .describedAs("page results had wrong size!")
                .hasSize(4);
        assertThat(page.getResults())
                .describedAs("page.getResults was wrong")
                .isEqualTo(Lists.newArrayList(0L, 1L, 2L, 3L));
        assertThat(page.moreResultsAvailable())
                .describedAs("page.moreResultsAvailable was wrong")
                .isFalse();

        page = BatchingVisitables.getFirstPage(visitor, 7);
        assertThat(page.getResults())
                .describedAs("page results had wrong size!")
                .hasSize(4);
        assertThat(page.getResults())
                .describedAs("page.getResults was wrong")
                .isEqualTo(Lists.newArrayList(0L, 1L, 2L, 3L));
        assertThat(page.moreResultsAvailable())
                .describedAs("page.moreResultsAvailable was wrong")
                .isFalse();

        assertThatThrownBy(() -> BatchingVisitables.getFirstPage(visitor, -1))
                .describedAs("Should not allow visiting -1 elements.")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testMinMax() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L));

        assertThat((long) BatchingVisitables.getMin(visitor))
                .describedAs("BatchingVisitables.getMin was wrong")
                .isEqualTo(0L);
        assertThat((long) BatchingVisitables.getMax(visitor))
                .describedAs("BatchingVisitables.getMax was wrong")
                .isEqualTo(3L);

        BatchingVisitable<Pair<Long, Long>> pairedVisitable = ListVisitor.create(
                Lists.newArrayList(Pair.create(0L, 0L), Pair.create(1L, 0L), Pair.create(0L, 1L), Pair.create(1L, 1L)));

        Ordering<Pair<Long, Long>> ordering = Pair.compareLhSide();
        assertThat(BatchingVisitables.getMin(pairedVisitable, ordering, null))
                .describedAs("BatchingVisitables.getMin was wrong")
                .isEqualTo(Pair.create(0L, 0L));
        assertThat(BatchingVisitables.getMax(pairedVisitable, ordering, null))
                .describedAs("BatchingVisitables.getMax was wrong")
                .isEqualTo(Pair.create(1L, 0L));
    }

    @Test
    public void testEmpty() {
        assertThat(BatchingVisitables.isEmpty(BatchingVisitables.emptyBatchingVisitable()))
                .describedAs("empty batching visitable should be empty!")
                .isTrue();
        assertThat(BatchingVisitables.count(BatchingVisitables.emptyBatchingVisitable()))
                .describedAs("empty batching visitable should be empty!")
                .isEqualTo(0);

        BatchingVisitable<Long> bv = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L));
        assertThat(BatchingVisitables.isEmpty(bv))
                .describedAs("non-empty batching visitable should not be empty!")
                .isFalse();
        assertThat(BatchingVisitables.count(bv))
                .describedAs("visitable had wrong size")
                .isEqualTo(4);
        assertThat(BatchingVisitables.emptyBatchingVisitable().batchAccept(1, AbortingVisitors.alwaysFalse()))
                .describedAs("empty visitable should always be true, even when told to visit an always-false place")
                .isTrue();
    }

    @Test
    public void testPageSize() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L));
        visitor.batchAccept(5, item -> {
            assertThat(item).describedAs("batched item had wrong size").hasSize(5);
            return false;
        });
        visitor.batchAccept(1, item -> {
            assertThat(item).describedAs("batched item had wrong size").hasSize(1);
            return false;
        });
        visitor.batchAccept(2, item -> {
            assertThat(item).describedAs("batched item had wrong size").hasSize(2);
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
            assertThat(item).describedAs("batched item had wrong size").hasSize(6);
            return true;
        });
    }

    @Test
    public void testBatchHints() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L));
        Function<List<Long>, List<String>> trans = input -> {
            assertThat(input).describedAs("batched item had wrong size").hasSize(2);
            return Lists.transform(input, Functions.toStringFunction());
        };
        BatchingVisitableView<String> visitable =
                BatchingVisitableView.of(visitor).transformBatch(trans).hintBatchSize(2);
        final Mutable<Boolean> hasTripped = Mutables.newMutable();
        visitable.batchAccept(10000, item -> {
            hasTripped.set(true);
            assertThat(item).describedAs("batched item had wrong size").hasSize(8);
            return false;
        });
        assertThat(hasTripped.get()).describedAs("should have been tripped!").isTrue();
    }

    @Test
    public void testImmutableCopyRespectsCustomBatchSize() {
        BatchingVisitableView<Void> bvv = BatchingVisitableView.of(bv);
        bvv.hintBatchSize(2).immutableCopy();

        verify(bv).batchAcceptSizeHint(eq(2), any());
    }

    @Test
    public void testImmutableCopyWithDefaultBatchSize() {
        BatchingVisitableView<Void> bvv = BatchingVisitableView.of(bv);
        bvv.immutableCopy();

        verify(bv).batchAcceptSizeHint(eq(BatchingVisitables.DEFAULT_BATCH_SIZE), any());
    }

    @Test
    public void testBatchWrap() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L));
        final Mutable<Boolean> hasTripped = Mutables.newMutable();
        AbortingVisitor<List<Object>, RuntimeException> bv = item -> {
            hasTripped.set(true);
            assertThat(item).describedAs("batched item had wrong size").hasSize(8);
            return false;
        };
        AbortingVisitor<List<Long>, RuntimeException> wrap = AbortingVisitors.wrapBatching(bv);
        BatchingVisitableView.of(visitor).batchAccept(1000, wrap);

        assertThat(hasTripped.get()).describedAs("should have been tripped!").isTrue();
    }

    @Test
    public void testBatchWrap2() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L));
        final Mutable<Boolean> hasTripped = Mutables.newMutable();
        AbortingVisitor<List<? extends Long>, RuntimeException> bv = item -> {
            hasTripped.set(true);
            assertThat(item).describedAs("batched item had wrong size").hasSize(8);
            return false;
        };
        AbortingVisitor<List<Long>, RuntimeException> wrap = AbortingVisitors.wrapBatching(bv);
        BatchingVisitableView.of(visitor).batchAccept(1000, wrap);

        assertThat(hasTripped.get()).describedAs("should have been tripped!").isTrue();
    }

    @Test
    public void testUnique() {
        BatchingVisitable<Long> visitor =
                ListVisitor.create(Lists.newArrayList(0L, 1L, 1L, 2L, 2L, 2L, 3L, 4L, 5L, 5L, 6L, 7L, 7L));
        BatchingVisitableView<Long> bv = BatchingVisitableView.of(visitor);
        assertThat(bv.unique().limit(4).immutableCopy())
                .describedAs("unexpected result for unique view")
                .isEqualTo(ImmutableList.of(0L, 1L, 2L, 3L));
    }

    @Test
    public void testLimit() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L));
        BatchingVisitableView<Long> limited = BatchingVisitableView.of(visitor).limit(3);
        ImmutableList<Long> copy = limited.immutableCopy();
        assertThat(copy).describedAs("limit produced unexpected result").isEqualTo(ImmutableList.of(0L, 1L, 2L));

        assertThat(limited.batchAccept(1, AbortingVisitors.alwaysFalse()))
                .describedAs("alwaysFalse should be false")
                .isFalse();
        assertThat(limited.batchAccept(1, AbortingVisitors.alwaysTrue()))
                .describedAs("alwaysTrue should be true")
                .isTrue();

        assertThat(limited.batchAccept(2, AbortingVisitors.alwaysTrue()))
                .describedAs("alwaysTrue should be true twice")
                .isTrue();
        assertThat(limited.batchAccept(3, AbortingVisitors.alwaysTrue()))
                .describedAs("alwaysTrue should be true thrice")
                .isTrue();

        CountingVisitor<Long> cv = new CountingVisitor<>();
        assertThat(limited.batchAccept(2, AbortingVisitors.batching(cv)))
                .describedAs("batchAccept should be true")
                .isTrue();
        assertThat(cv.count).describedAs("CountingVisitor had wrong count").isEqualTo(3);
        assertThat(limited.batchAccept(3, AbortingVisitors.batching(cv)))
                .describedAs("batchAccept should be true")
                .isTrue();
        assertThat(cv.count).describedAs("CountingVisitor had wrong count").isEqualTo(6);
        assertThat(limited.batchAccept(4, AbortingVisitors.batching(cv)))
                .describedAs("batchAccept should be true")
                .isTrue();
        assertThat(cv.count).describedAs("CountingVisitor had wrong count").isEqualTo(9);

        assertThat(limited.skip(3).batchAccept(1, AbortingVisitors.alwaysFalse()))
                .describedAs("batchAccept should be trivially true after everything was skipped")
                .isTrue();

        LimitVisitor<Long> lv = new LimitVisitor<>(3);
        assertThat(limited.batchAccept(1, AbortingVisitors.batching(lv)))
                .describedAs("batchAccept should be false")
                .isFalse();
        lv = new LimitVisitor<>(4);
        assertThat(limited.batchAccept(1, AbortingVisitors.batching(lv)))
                .describedAs("batchAccept should be true")
                .isTrue();
        lv = new LimitVisitor<>(2);
        assertThat(limited.batchAccept(1, AbortingVisitors.batching(lv)))
                .describedAs("batchAccept should be false")
                .isFalse();

        assertThat(limited.hintBatchSize(10).batchAccept(1, AbortingVisitors.alwaysFalse()))
                .describedAs("batchAccept should be false")
                .isFalse();
        assertThat(limited.hintBatchSize(10).skip(3).batchAccept(1, AbortingVisitors.alwaysFalse()))
                .describedAs("batchAccept should be trivially true after everything was skipped")
                .isTrue();

        assertThat(limited.limit(2).limit(4).size())
                .describedAs("smaller limits should precede larger limits")
                .isEqualTo(2);
        assertThat(limited.limit(4).limit(2).size())
                .describedAs("smaller limits should precede larger limits")
                .isEqualTo(2);
        assertThat(limited.limit(4).size())
                .describedAs("limited size shouldn't be greater than the original size!")
                .isEqualTo(3);
    }

    @Test
    public void testConcat() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L));
        BatchingVisitableView<Long> limited = BatchingVisitableView.of(visitor).limit(3);
        BatchingVisitableView<Long> concat = BatchingVisitables.concat(limited, visitor);
        assertThat(concat.batchAccept(2, AbortingVisitors.batching(AbortingVisitors.alwaysTrue())))
                .describedAs("concatenated batchAccept should be true")
                .isTrue();
    }

    static class CountingVisitor<T> implements AbortingVisitor<T, RuntimeException> {
        long count = 0;

        @Override
        public boolean visit(T _item) throws RuntimeException {
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
        public boolean visit(T _item) throws RuntimeException {
            count++;
            return count < limit;
        }
    }

    @Test
    public void testAnyAndAllForNonEmptyLists() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L));
        BatchingVisitableView<Long> bv = BatchingVisitableView.of(visitor);
        assertThat(bv.any(Predicates.alwaysTrue()))
                .describedAs("any(true) should be true")
                .isTrue();
        assertThat(bv.all(Predicates.alwaysTrue()))
                .describedAs("all(true) should be true")
                .isTrue();

        assertThat(bv.any(Predicates.alwaysFalse()))
                .describedAs("any(false) should be false")
                .isFalse();
        assertThat(bv.all(Predicates.alwaysFalse()))
                .describedAs("all(false) should be false")
                .isFalse();
    }

    @Test
    public void testAnyAndAllForEmptyLists() {
        BatchingVisitable<Long> visitor = BatchingVisitables.emptyBatchingVisitable();
        BatchingVisitableView<Long> bv = BatchingVisitableView.of(visitor);
        assertThat(bv.any(Predicates.alwaysTrue()))
                .describedAs("any(empty-set-of-trues) should be false")
                .isFalse();
        assertThat(bv.all(Predicates.alwaysTrue()))
                .describedAs("all(empty-set-of-trues) should be true")
                .isTrue();

        assertThat(bv.any(Predicates.alwaysFalse()))
                .describedAs("any(empty-set-of-falses) should be false")
                .isFalse();
        assertThat(bv.all(Predicates.alwaysFalse()))
                .describedAs("all(empty-set-of-falses) should be true")
                .isTrue();
    }

    @Test
    public void testHintPageSize() {
        InfiniteVisitable infinite = new InfiniteVisitable();
        BatchingVisitableView<Long> bv = BatchingVisitableView.of(infinite);
        long first = bv.filter(new TakeEvery<>(100)).getFirst();
        assertThat(first).describedAs("first element returned was wrong").isEqualTo(99L);
        assertThat(infinite.count)
                .describedAs("count of InfiniteVisitable didn't match")
                .isEqualTo(100L);

        first = bv.filter(new TakeEvery<>(100)).hintBatchSize(100).getFirst();
        assertThat(first).describedAs("first element returned was wrong").isEqualTo(199L);
        assertThat(infinite.count)
                .describedAs("count of InfiniteVisitable didn't match")
                .isEqualTo(200L);
    }

    static class InfiniteVisitable extends AbstractBatchingVisitable<Long> {
        long count = 0;

        @Override
        protected <K extends Exception> void batchAcceptSizeHint(int _batchSizeHint, ConsistentVisitor<Long, K> bv)
                throws K {
            while (bv.visitOne(count++)) {
                /* */
            }
        }
    }

    static class TakeEvery<T> implements Predicate<T> {
        final long numToSkip;
        long count = 0;

        TakeEvery(long numToSkip) {
            this.numToSkip = numToSkip;
        }

        @Override
        public boolean apply(@Nullable T _input) {
            return ++count % numToSkip == 0;
        }
    }

    @Test
    public void testSkip() {
        BatchingVisitable<Long> visitor = ListVisitor.create(Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L));
        ImmutableList<Long> copy = BatchingVisitableView.of(visitor).skip(5).immutableCopy();
        assertThat(copy).describedAs("unexpected list remnants after skipping").isEqualTo(ImmutableList.of(5L, 6L, 7L));
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
        protected <K extends Exception> void batchAcceptSizeHint(int batchSize, ConsistentVisitor<T, K> bv) throws K {
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
        BatchingVisitableView<Long> view =
                BatchingVisitables.visitWhile(BatchingVisitableView.of(visitable), input -> input.longValue() < 5L);
        assertThat(view.size())
                .describedAs("visitWhile visited the wrong number of elements")
                .isEqualTo(5L);
        assertThat(view.getFirst().longValue())
                .describedAs("visitWhile visited the wrong element first")
                .isEqualTo(0L);
        assertThat(view.getLast().longValue())
                .describedAs("visitWhile visited the wrong element last")
                .isEqualTo(4L);
        assertThat(view.immutableCopy().containsAll(ImmutableSet.of(0L, 1L, 2L, 3L, 4L)))
                .describedAs("visitWhile visited the wrong elements")
                .isTrue();

        visitable = ListVisitor.create(Lists.reverse(longList));
        view = BatchingVisitables.visitWhile(BatchingVisitableView.of(visitable), input -> input.longValue() >= 5L);
        assertThat(view.size())
                .describedAs("visitWhile visited the wrong number of elements")
                .isEqualTo(5L);
        assertThat(view.getFirst().longValue())
                .describedAs("visitWhile visited the wrong element first")
                .isEqualTo(9L);
        assertThat(view.getLast().longValue())
                .describedAs("visitWhile visited the wrong element last")
                .isEqualTo(5L);
        assertThat(view.immutableCopy().containsAll(ImmutableSet.of(5L, 6L, 7L, 8L, 9L)))
                .describedAs("visitWhile visited the wrong elements")
                .isTrue();
    }

    @Test
    public void testFlatten() {
        List<String> firstChars = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            firstChars.add("" + (char) ('a' + i));
        }
        BatchingVisitable<String> outerVisitable = BatchingVisitableFromIterable.create(firstChars);
        BatchingVisitableView<BatchingVisitable<String>> bv = BatchingVisitables.transform(outerVisitable, prefix -> {
            List<String> innerChars = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                innerChars.add(prefix + (char) ('0' + i));
            }
            return BatchingVisitableFromIterable.create(innerChars);
        });
        UnmodifiableIterator<String> iter =
                BatchingVisitables.flatten(7, bv).immutableCopy().iterator();
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                assertThat(iter.next())
                        .describedAs("unexpected flattened result")
                        .isEqualTo("" + (char) ('a' + i) + (char) ('0' + j));
            }
        }
    }
}

/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.sweep;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.Test;

import com.google.common.base.Equivalence;
import com.google.common.collect.ImmutableList;

public class EquivalenceCountingIteratorShould {
    private static final int LIMIT = Integer.MAX_VALUE;

    @Test
    public void retainTheSameElements() {
        ImmutableList<Integer> original = ImmutableList.of(2, 4, 6);
        EquivalenceCountingIterator<Integer> iterator = sameObjectEquivalenceIterator(original);

        assertThat(ImmutableList.copyOf(iterator)).isEqualTo(original);
    }

    @Test
    public void returnCorrectSizeWhenNoDuplicates() {
        EquivalenceCountingIterator<Integer> iterator = sameObjectEquivalenceIterator(ImmutableList.of(2, 4, 6));
        advanceIterator(iterator);

        assertThat(iterator.size()).isEqualTo(3);
    }

    @Test
    public void returnCorrectSizeWhenHasDuplicates() {
        EquivalenceCountingIterator<Integer> iterator = sameObjectEquivalenceIterator(ImmutableList.of(2, 2, 4, 6, 6));
        advanceIterator(iterator);

        assertThat(iterator.size()).isEqualTo(3);
    }

    @Test
    public void failHasNextAndNextThrowsIfLimitReached() {
        EquivalenceCountingIterator<Integer> iterator = sameObjectEquivalenceIterator(
                ImmutableList.of(2, 2, 4, 4, 6, 6, 8), 2);

        assertThat(iterator.next()).isEqualTo(2);
        assertThat(iterator.next()).isEqualTo(2);
        assertThat(iterator.next()).isEqualTo(4);
        assertThat(iterator.next()).isEqualTo(4);
        assertThat(iterator.hasNext()).isFalse();
        assertThatThrownBy(() -> iterator.next())
                .isExactlyInstanceOf(NoSuchElementException.class)
                .withFailMessage("Reached limit");
    }

    @Test
    public void returnCorrectLastElement() {
        EquivalenceCountingIterator<Integer> iterator = sameObjectEquivalenceIterator(
                ImmutableList.of(2, 2, 4, 4, 6, 6, 8), 2);
        advanceIterator(iterator);

        assertThat(iterator.lastItem()).isEqualTo(4);
    }

    @Test
    public void workCorrectlyForNonTrivialEquivalence() {
        EquivalenceCountingIterator<String> iterator = new EquivalenceCountingIterator<>(
                ImmutableList.of("aa", "ab", "bc", "bd", "de", "ef", "ee", "ez", "ae").iterator(),
                LIMIT,
                new SameFirstLetter());
        advanceIterator(iterator);

        assertThat(iterator.lastItem()).isEqualTo("ae");
        assertThat(iterator.size()).isEqualTo(5);
    }

    private EquivalenceCountingIterator<Integer> sameObjectEquivalenceIterator(List<Integer> values) {
        return sameObjectEquivalenceIterator(values, LIMIT);
    }

    private EquivalenceCountingIterator<Integer> sameObjectEquivalenceIterator(List<Integer> values, int limit) {
        Iterator<Integer> iterator = ImmutableList.copyOf(values).iterator();
        return new EquivalenceCountingIterator(iterator, limit, new SameObject<>());
    }

    private void advanceIterator(Iterator iterator) {
        while (iterator.hasNext()) {
            iterator.next();
        }
    }

    private static class SameObject<T> extends Equivalence<T> {
        @Override
        protected boolean doEquivalent(T fst, T snd) {
            return false;
        }

        @Override
        protected int doHash(T anything) {
            return 0;
        }
    }

    private static class SameFirstLetter extends Equivalence<String> {
        @Override
        protected boolean doEquivalent(String fst, String snd) {
            return fst.substring(0, 1).equals(snd.substring(0, 1));
        }

        @Override
        protected int doHash(String anything) {
            return 0;
        }
    }
}

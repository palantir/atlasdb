/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class CountingIteratorShould {
    @Test
    public void retainTheSameElements() {
        ImmutableList<Integer> original = ImmutableList.of(2, 4, 6);
        CountingIterator<Integer> countingIterator = makeCountingIterator(original);

        List copied = ImmutableList.copyOf(countingIterator);

        assertThat(copied, equalTo(original));
    }

    @Test
    public void returnCorrectSize() {
        CountingIterator<Integer> countingIterator = makeCountingIterator(ImmutableList.of(2, 4, 6));

        advanceIterator(countingIterator);
        int size = countingIterator.size();

        assertThat(size, equalTo(3));
    }

    @Test(expected = IllegalStateException.class)
    public void failSizeIfHasNext() {
        CountingIterator<Integer> countingIterator = makeCountingIterator(ImmutableList.of(2, 4, 6));

        countingIterator.size();
    }

    @Test
    public void returnCorrectLastElement() {
        CountingIterator<Integer> countingIterator = makeCountingIterator(ImmutableList.of(2, 4, 6));

        advanceIterator(countingIterator);
        Integer lastItem = countingIterator.lastItem();

        assertThat(lastItem, equalTo(6));
    }

    @Test(expected = IllegalStateException.class)
    public void failLastElemntIfHasNext() {
        CountingIterator<Integer> countingIterator = makeCountingIterator(ImmutableList.of(2, 4, 6));

        countingIterator.lastItem();
    }

    private CountingIterator<Integer> makeCountingIterator(List<Integer> values) {
        Iterator<Integer> iterator = ImmutableList.copyOf(values).iterator();
        return new CountingIterator(iterator);
    }

    private void advanceIterator(Iterator iterator) {
        while (iterator.hasNext()) {
            iterator.next();
        }
    }
}

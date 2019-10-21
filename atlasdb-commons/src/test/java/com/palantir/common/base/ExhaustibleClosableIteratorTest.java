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

package com.palantir.common.base;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

public final class ExhaustibleClosableIteratorTest {

    @Test
    public void testExhaustibleClosableIterator() {
        ExhaustibleClosableIterator<Integer> iterator =
                new ExhaustibleClosableIterator<>(ClosableIterators.wrap(ImmutableList.of(0, 1, 2).iterator()));
        assertThat(iterator.isExhausted()).isFalse();
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next()).isEqualTo(0);

        assertThat(iterator.isExhausted()).isFalse();
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next()).isEqualTo(1);

        assertThat(iterator.isExhausted()).isFalse();
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next()).isEqualTo(2);

        assertThat(iterator.isExhausted()).isFalse();
        assertThat(iterator.hasNext()).isFalse();
        assertThat(iterator.isExhausted()).isTrue();
    }
}

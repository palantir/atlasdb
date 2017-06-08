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

package com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class PageJoiningIteratorTest {

    @Test
    public void emptySequenceStaysEmpty() {
        // nothing -> nothing
        assertTrue(transform(2, ImmutableList.of()).isEmpty());
    }

    @Test
    public void oneEmptyListBecomesEmptySequence() {
        // [] -> nothing
        assertTrue(transform(2, ImmutableList.of(ImmutableList.of())).isEmpty());
    }

    @Test
    public void manyEmptyListsBecomeEmptySequence() {
        // [], [], [] -> nothing
        assertTrue(
                transform(2, ImmutableList.of(ImmutableList.of(), ImmutableList.of(), ImmutableList.of())).isEmpty());
    }

    @Test
    public void pagesThatAreBigEnoughArePreservedAsIs() {
        // min page size 2: [a, b], [c, d] -> [a, b], [c, d]
        assertEquals(
                ImmutableList.of(ImmutableList.of("a", "b"), ImmutableList.of("c", "d")),
                transform(2, ImmutableList.of(ImmutableList.of("a", "b"), ImmutableList.of("c", "d"))));
    }

    @Test
    public void smallPagesAreJoined() {
        // min page size 2: [a], [], [b, c], [d] -> [a, b, c], [d]
        assertEquals(
                ImmutableList.of(ImmutableList.of("a", "b", "c"), ImmutableList.of("d")),
                transform(2, ImmutableList.of(
                        ImmutableList.of("a"), ImmutableList.of(), ImmutableList.of("b", "c"), ImmutableList.of("d"))));
    }

    private static List<List<String>> transform(int minPageSize, List<List<String>> inputPages) {
        return ImmutableList.copyOf(new PageJoiningIterator<>(inputPages.iterator(), minPageSize));
    }

}

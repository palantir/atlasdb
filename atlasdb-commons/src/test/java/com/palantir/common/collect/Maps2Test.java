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

package com.palantir.common.collect;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class Maps2Test {
    @Test
    public void innerJoinCorrectIfAllInnerKeysMatch() {
        Map<String, Integer> firstMap = ImmutableMap.of("foo", 1, "bar", 2);
        Map<Integer, Character> secondMap = ImmutableMap.of(1, 'q', 2, '!');

        assertThat(Maps2.innerJoin(firstMap, secondMap)).isEqualTo(ImmutableMap.of("foo", 'q', "bar", '!'));
    }

    @Test
    public void innerJoinReturnsEmptyIfFirstMapIsEmpty() {
        Map<String, Integer> firstMap = ImmutableMap.of();
        Map<Integer, Long> secondMap = ImmutableMap.of(1, -1L, 2, -2L);

        assertThat(Maps2.innerJoin(firstMap, secondMap)).isEmpty();
    }

    @Test
    public void innerJoinReturnsEmptyIfSecondMapIsEmpty() {
        Map<String, Integer> firstMap = ImmutableMap.of("baz", 3, "quux", 4);
        Map<Integer, Long> secondMap = ImmutableMap.of();

        Map<String, Long> joinedMap = Maps2.innerJoin(firstMap, secondMap);
        assertThat(Maps2.innerJoin(firstMap, secondMap)).isEmpty();
    }

    @Test
    public void innerJoinReturnsEmptyIfValuesOfFirstMapAndKeysOfSecondAreDisjoint() {
        Map<Integer, String> firstMap = ImmutableMap.of(1, "alpha", 2, "omega");
        Map<String, Integer> secondMap = ImmutableMap.of("beta", 11, "zeta", 12);

        assertThat(Maps2.innerJoin(firstMap, secondMap)).isEmpty();
    }

    @Test
    public void innerJoinCorrectIfOnlySomeInnerKeysMatch() {
        Map<Integer, String> firstMap = ImmutableMap.of(1, "alpha", 2, "omega");
        Map<String, Integer> secondMap = ImmutableMap.of("alpha", 11, "delta", 12);

        assertThat(Maps2.innerJoin(firstMap, secondMap)).isEqualTo(ImmutableMap.of(1, 11));
    }

    @Test
    public void innerJoinComparesObjectsOnEquals() {
        List<String> firstList = ImmutableList.of("a", "b", "ccc");
        List<String> secondList = Lists.newArrayList(firstList);
        assertThat(firstList).isNotSameAs(secondList);

        Map<String, List<String>> firstMap = ImmutableMap.of("galaxy", firstList);
        Map<List<String>, String> secondMap = ImmutableMap.of(secondList, "towel");

        assertThat(Maps2.innerJoin(firstMap, secondMap)).isEqualTo(ImmutableMap.of("galaxy", "towel"));
    }
}

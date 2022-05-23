/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.Test;

public class CellLoaderTest {

    @Test
    public void flattenReadOnlyLists() {
        List<List<String>> lists = ImmutableList.of(
                ImmutableList.of("A1"),
                ImmutableList.of("B1", "B2", "B3"),
                ImmutableList.of(),
                ImmutableList.of("D1", "D2"));
        List<String> zipped = CellLoader.flattenReadOnlyLists(lists);
        assertThat(zipped)
                .hasSize(6)
                .containsExactly("A1", "B1", "B2", "B3", "D1", "D2")
                .element(5)
                .isEqualTo("D2");

        assertThat(zipped.get(4)).isEqualTo("D1");
        assertThatThrownBy(() -> zipped.get(-1)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> zipped.get(6)).isInstanceOf(IndexOutOfBoundsException.class);
    }
}

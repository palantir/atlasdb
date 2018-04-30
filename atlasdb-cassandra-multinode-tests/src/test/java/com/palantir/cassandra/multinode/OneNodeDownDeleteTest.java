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
package com.palantir.cassandra.multinode;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import com.google.common.collect.ImmutableMultimap;
import com.palantir.common.exception.AtlasDbDependencyException;

public class OneNodeDownDeleteTest {

    @Test
    public void deletingThrows() {
        assertThatThrownBy(() -> OneNodeDownTestSuite.kvs.delete(OneNodeDownTestSuite.TEST_TABLE,
                ImmutableMultimap.of(OneNodeDownTestSuite.CELL_1_1, OneNodeDownTestSuite.DEFAULT_TIMESTAMP)))
                .isInstanceOf(AtlasDbDependencyException.class);
    }
}

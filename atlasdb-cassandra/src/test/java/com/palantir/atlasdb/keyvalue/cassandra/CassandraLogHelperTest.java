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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.Set;
import org.apache.cassandra.thrift.TokenRange;
import org.junit.Test;

public class CassandraLogHelperTest {
    private static final String TOKEN_1 = "i-am-a-token";
    private static final String TOKEN_2 = "this-is-another-token";
    private static final String TOKEN_3 = "yet-another-token";

    private static final TokenRange TOKEN_RANGE_1_TO_2 = new TokenRange(TOKEN_1, TOKEN_2, ImmutableList.of());
    private static final TokenRange TOKEN_RANGE_2_TO_3 = new TokenRange(TOKEN_2, TOKEN_3, ImmutableList.of());

    @Test
    public void tokenRangeHashesHashesIndividualRanges() {
        Set<String> expectedHashes = Sets.union(
                CassandraLogHelper.tokenRangeHashes(ImmutableSet.of(TOKEN_RANGE_1_TO_2)),
                CassandraLogHelper.tokenRangeHashes(ImmutableSet.of(TOKEN_RANGE_2_TO_3)));
        assertThat(CassandraLogHelper.tokenRangeHashes(ImmutableSet.of(TOKEN_RANGE_1_TO_2, TOKEN_RANGE_2_TO_3)))
                .containsExactlyInAnyOrderElementsOf(expectedHashes);
    }

    @Test
    public void tokenRangeHashesDoesNotPublishActualValues() {
        assertThat(Iterables.getOnlyElement(CassandraLogHelper.tokenRangeHashes(ImmutableSet.of(TOKEN_RANGE_1_TO_2))))
                .doesNotContain(TOKEN_1)
                .doesNotContain(TOKEN_2);
    }
}

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

package com.palantir.atlasdb.cassandra.backup;

import static com.google.common.collect.ImmutableRangeSet.toImmutableRangeSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.CassandraTokenRanges;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import java.util.Set;
import org.junit.Test;

public class CqlMetadataTest {
    @Test
    public void tokenRange() {
        String token1 = "1df388e2a10c81a4339ab9304497385b";
        String token2 = "974ef05bdfaf14b88cbe12bce50e5023";
        String token3 = "d9ce4afeafac5781fb101e8bef4703f8";
        TokenRange firstRange = CassandraTokenRanges.create(token1, token2);
        TokenRange secondRange = CassandraTokenRanges.create(token2, token3);
        TokenRange wraparoundRange = CassandraTokenRanges.create(token3, token1);
        Token firstToken = firstRange.getStart();
        Token secondToken = secondRange.getStart();
        Token thirdToken = wraparoundRange.getStart();

        Metadata metadata = mock(Metadata.class);
        when(metadata.getTokenRanges()).thenReturn(ImmutableSet.of(firstRange, secondRange, wraparoundRange));

        CqlMetadata cqlMetadata = new CqlMetadata(metadata);
        Set<Range<LightweightOppToken>> tokenRanges = cqlMetadata.getTokenRanges();
        assertThat(tokenRanges)
                .containsExactlyInAnyOrder(
                        Range.closedOpen(
                                LightweightOppToken.serialize(firstToken), LightweightOppToken.serialize(secondToken)),
                        Range.closedOpen(
                                LightweightOppToken.serialize(secondToken), LightweightOppToken.serialize(thirdToken)),
                        Range.atLeast(LightweightOppToken.serialize(thirdToken)),
                        Range.lessThan(LightweightOppToken.serialize(firstToken)));

        // Turning this into a RangeSet should give the complete range (-inf, +inf)
        RangeSet<LightweightOppToken> fullTokenRing = tokenRanges.stream().collect(toImmutableRangeSet());
        assertThat(fullTokenRing.asRanges()).containsExactly(Range.all());
    }
}

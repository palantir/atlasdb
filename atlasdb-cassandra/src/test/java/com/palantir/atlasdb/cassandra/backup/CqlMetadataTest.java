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
import static org.mockito.Mockito.when;

import com.datastax.driver.core.CassandraTokenRanges;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import com.palantir.common.streams.KeyedStream;
import java.util.Set;
import java.util.TreeMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CqlMetadataTest {
    private static final String TOKEN_1 = "1df388e2a10c81a4339ab9304497385b";
    private static final String TOKEN_2 = "974ef05bdfaf14b88cbe12bce50e5023";
    private static final String TOKEN_3 = "d9ce4afeafac5781fb101e8bef4703f8";
    private static final TokenRange FIRST_RANGE = CassandraTokenRanges.create(TOKEN_1, TOKEN_2);
    private static final TokenRange SECOND_RANGE = CassandraTokenRanges.create(TOKEN_2, TOKEN_3);
    private static final TokenRange WRAPAROUND_RANGE = CassandraTokenRanges.create(TOKEN_3, TOKEN_1);
    private static final Token FIRST_TOKEN = FIRST_RANGE.getStart();
    private static final Token SECOND_TOKEN = SECOND_RANGE.getStart();
    private static final Token THIRD_TOKEN = WRAPAROUND_RANGE.getStart();

    @Mock
    private Metadata metadata;

    private CqlMetadata cqlMetadata;

    @Before
    public void setUp() {
        when(metadata.getTokenRanges()).thenReturn(ImmutableSet.of(FIRST_RANGE, SECOND_RANGE, WRAPAROUND_RANGE));
        cqlMetadata = new CqlMetadata(metadata);
    }

    @Test
    public void testGetTokenRanges() {
        Set<Range<LightweightOppToken>> tokenRanges = cqlMetadata.getTokenRanges();
        assertThat(tokenRanges)
                .containsExactlyInAnyOrder(
                        Range.closedOpen(
                                LightweightOppToken.serialize(FIRST_TOKEN),
                                LightweightOppToken.serialize(SECOND_TOKEN)),
                        Range.closedOpen(
                                LightweightOppToken.serialize(SECOND_TOKEN),
                                LightweightOppToken.serialize(THIRD_TOKEN)),
                        Range.atLeast(LightweightOppToken.serialize(THIRD_TOKEN)),
                        Range.lessThan(LightweightOppToken.serialize(FIRST_TOKEN)));

        // Turning this into a RangeSet should give the complete range (-inf, +inf)
        RangeSet<LightweightOppToken> fullTokenRing = tokenRanges.stream().collect(toImmutableRangeSet());
        assertThat(fullTokenRing.asRanges()).containsExactly(Range.all());
    }

    @Test
    public void canGetTokenRangesByEnd() {
        Set<Range<LightweightOppToken>> tokenRanges = cqlMetadata.getTokenRanges();

        TreeMap<LightweightOppToken, Range<LightweightOppToken>> tokenRangesByEnd = KeyedStream.of(tokenRanges)
                .mapKeys(LightweightOppToken::getUpper)
                .collectTo(TreeMap::new);

        assertThat(tokenRangesByEnd).hasSize(4);
    }
}

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.CassandraTokenRanges;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TokenRange;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
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
public class ClusterMetadataUtilsTest {
    // TODO(gs): copied from CqlMetadataTest
    private static final String TOKEN_1 = "1df388e2a10c81a4339ab9304497385b";
    private static final String TOKEN_2 = "974ef05bdfaf14b88cbe12bce50e5023";
    private static final String TOKEN_3 = "d9ce4afeafac5781fb101e8bef4703f8";
    private static final TokenRange FIRST_RANGE = CassandraTokenRanges.create(TOKEN_1, TOKEN_2);
    private static final TokenRange SECOND_RANGE = CassandraTokenRanges.create(TOKEN_2, TOKEN_3);
    private static final TokenRange WRAPAROUND_RANGE = CassandraTokenRanges.create(TOKEN_3, TOKEN_1);

    @Mock
    private Metadata metadata;

    private TreeMap<LightweightOppToken, Range<LightweightOppToken>> tokenRangesByEnd;
    private CqlMetadata cqlMetadata;

    @Before
    public void setUp() {
        when(metadata.getTokenRanges()).thenReturn(ImmutableSet.of(FIRST_RANGE, SECOND_RANGE, WRAPAROUND_RANGE));
        cqlMetadata = new CqlMetadata(metadata);

        tokenRangesByEnd = KeyedStream.of(cqlMetadata.getTokenRanges())
                .mapKeys(ClusterMetadataUtilsTest::getUpper)
                .collectTo(TreeMap::new);
    }

    @Test
    public void testMinTokenRangeIsLatestEnding() {
        LightweightOppToken duplicatedStartKey = getToken("0001");
        LightweightOppToken normalEndKey = getToken("000101");
        Range<LightweightOppToken> nested = Range.closed(duplicatedStartKey, normalEndKey);
        Range<LightweightOppToken> outer = Range.atLeast(duplicatedStartKey);
        assertThat(ClusterMetadataUtils.findLatestEndingRange(nested, outer)).isEqualTo(outer);
    }

    @Test
    public void testSmallTokenRangeBeforeFirstVnode() {
        LightweightOppToken partitionKeyToken = getToken("0010");

        Set<Range<LightweightOppToken>> tokenRanges = ClusterMetadataUtils.getMinimalSetOfRangesForTokens(
                        ImmutableSet.of(partitionKeyToken), tokenRangesByEnd)
                .asRanges();
        assertThat(tokenRanges).hasSize(1);
        Range<LightweightOppToken> onlyRange = tokenRanges.iterator().next();
        assertThat(onlyRange.hasLowerBound()).isFalse();
        assertThat(onlyRange.upperEndpoint()).isEqualTo(partitionKeyToken);
    }

    @Test
    public void testSmallTokenRangeOnVnode() {
        // Construction of tokenRangesByEnd gives us +inf as the "first" key, so we actually want the second range here
        // TODO(gs): add extra testing to ensure this is fine
        LightweightOppToken firstEndToken = tokenRangesByEnd.higherKey(tokenRangesByEnd.firstKey());
        LightweightOppToken secondEndToken = tokenRangesByEnd.higherKey(firstEndToken);
        Set<Range<LightweightOppToken>> tokenRanges = ClusterMetadataUtils.getMinimalSetOfRangesForTokens(
                        ImmutableSet.of(secondEndToken), tokenRangesByEnd)
                .asRanges();
        assertThat(tokenRanges).hasSize(1);
        Range<LightweightOppToken> onlyRange = tokenRanges.iterator().next();
        assertThat(onlyRange.lowerEndpoint()).isEqualTo(firstEndToken);
        assertThat(onlyRange.upperEndpoint()).isEqualTo(secondEndToken);
    }

    @Test
    public void testRemoveNestedWraparoundAndNonWrapRanges() {
        LightweightOppToken duplicatedStartKey = getToken("ff");
        LightweightOppToken nonWrapAround = getToken("ff01");
        Range<LightweightOppToken> nonWrapAroundRange = Range.closed(duplicatedStartKey, nonWrapAround);
        Range<LightweightOppToken> wrapAroundRange = Range.atLeast(duplicatedStartKey);
        assertThat(ClusterMetadataUtils.findLatestEndingRange(nonWrapAroundRange, wrapAroundRange))
                .isEqualTo(wrapAroundRange);
    }

    // TODO(gs): duplicated in a few places now
    private static LightweightOppToken getUpper(Range<LightweightOppToken> range) {
        return range.hasUpperBound()
                ? range.upperEndpoint()
                : LightweightOppToken.serialize(CassandraTokenRanges.maxToken());
    }

    private LightweightOppToken getToken(String hexBinary) {
        return LightweightOppToken.serialize(CassandraTokenRanges.getToken(hexBinary));
    }
}

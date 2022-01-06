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
public class ClusterMetadataUtilsTest {
    private static final String TOKEN_1 = "1df388e2a10c81a4339ab9304497385b";
    private static final String TOKEN_2 = "974ef05bdfaf14b88cbe12bce50e5023";
    private static final String TOKEN_3 = "d9ce4afeafac5781fb101e8bef4703f8";
    private static final TokenRange FIRST_RANGE = CassandraTokenRanges.create(TOKEN_1, TOKEN_2);
    private static final TokenRange SECOND_RANGE = CassandraTokenRanges.create(TOKEN_2, TOKEN_3);
    private static final TokenRange WRAPAROUND_RANGE = CassandraTokenRanges.create(TOKEN_3, TOKEN_1);
    private static final Token SECOND_TOKEN = SECOND_RANGE.getStart();
    private static final Token THIRD_TOKEN = WRAPAROUND_RANGE.getStart();

    @Mock
    private Metadata metadata;

    private TreeMap<LightweightOppToken, Range<LightweightOppToken>> tokenRangesByStart;

    @Before
    public void setUp() {
        when(metadata.getTokenRanges()).thenReturn(ImmutableSet.of(FIRST_RANGE, SECOND_RANGE, WRAPAROUND_RANGE));
        CqlMetadata cqlMetadata = new CqlMetadata(metadata);

        tokenRangesByStart = KeyedStream.of(cqlMetadata.getTokenRanges())
                .mapKeys(LightweightOppToken::getLowerExclusive)
                .collectTo(TreeMap::new);
    }

    @Test
    public void testMinimalSetOfTokenRanges() {
        LightweightOppToken partitionKeyToken = getToken("9000");
        LightweightOppToken lastTokenBeforePartitionKey = tokenRangesByStart.lowerKey(partitionKeyToken);

        RangeSet<LightweightOppToken> tokenRanges = ClusterMetadataUtils.getMinimalSetOfRangesForTokens(
                ImmutableSet.of(partitionKeyToken), tokenRangesByStart);
        assertThat(tokenRanges.asRanges()).hasSize(1);
        Range<LightweightOppToken> onlyRange = tokenRanges.asRanges().iterator().next();
        assertThat(onlyRange.lowerEndpoint()).isEqualTo(lastTokenBeforePartitionKey);
        assertThat(onlyRange.upperEndpoint()).isEqualTo(partitionKeyToken);
    }

    @Test
    public void testMinTokenIsStart() {
        LightweightOppToken nestedEndKey = getToken("0001");
        LightweightOppToken outerEndKey = getToken("0002");
        Range<LightweightOppToken> nested = Range.atMost(nestedEndKey);
        Range<LightweightOppToken> outer = Range.atMost(outerEndKey);
        assertThat(ClusterMetadataUtils.findLatestEndingRange(nested, outer)).isEqualTo(outer);
    }

    @Test
    public void testUnboundedRangeIsLatestEnding() {
        LightweightOppToken duplicatedStartKey = getToken("0001");
        LightweightOppToken normalEndKey = getToken("000101");
        Range<LightweightOppToken> nested = Range.openClosed(duplicatedStartKey, normalEndKey);
        Range<LightweightOppToken> outer = Range.greaterThan(duplicatedStartKey);
        assertThat(ClusterMetadataUtils.findLatestEndingRange(nested, outer)).isEqualTo(outer);
    }

    @Test
    public void testSmallTokenRangeDedupe() {
        LightweightOppToken partitionKeyToken1 = getToken("9000");
        LightweightOppToken partitionKeyToken2 = getToken("9001");
        Set<Range<LightweightOppToken>> tokenRanges = ClusterMetadataUtils.getMinimalSetOfRangesForTokens(
                        ImmutableSet.of(partitionKeyToken1, partitionKeyToken2), tokenRangesByStart)
                .asRanges();
        assertThat(tokenRanges).hasSize(1);
        Range<LightweightOppToken> onlyRange = tokenRanges.iterator().next();
        assertThat(onlyRange.lowerEndpoint()).isEqualTo(tokenRangesByStart.lowerKey(partitionKeyToken1));
        assertThat(onlyRange.upperEndpoint()).isEqualTo(partitionKeyToken2);
    }

    @Test
    public void testSmallTokenRangeBeforeFirstVnode() {
        LightweightOppToken partitionKeyToken = getToken("0010");

        Set<Range<LightweightOppToken>> tokenRanges = ClusterMetadataUtils.getMinimalSetOfRangesForTokens(
                        ImmutableSet.of(partitionKeyToken), tokenRangesByStart)
                .asRanges();
        assertThat(tokenRanges).hasSize(1);
        Range<LightweightOppToken> onlyRange = tokenRanges.iterator().next();
        assertThat(onlyRange.hasLowerBound()).isFalse();
        assertThat(onlyRange.upperEndpoint()).isEqualTo(partitionKeyToken);
    }

    @Test
    public void testSmallTokenRangeOnVnode() {
        LightweightOppToken firstEndToken = tokenRangesByStart.higherKey(tokenRangesByStart.firstKey());
        LightweightOppToken secondEndToken = tokenRangesByStart.higherKey(firstEndToken);
        Set<Range<LightweightOppToken>> tokenRanges = ClusterMetadataUtils.getMinimalSetOfRangesForTokens(
                        ImmutableSet.of(secondEndToken), tokenRangesByStart)
                .asRanges();
        assertThat(tokenRanges).hasSize(1);
        Range<LightweightOppToken> onlyRange = tokenRanges.iterator().next();
        assertThat(onlyRange.lowerEndpoint()).isEqualTo(firstEndToken);
        assertThat(onlyRange.upperEndpoint()).isEqualTo(secondEndToken);
    }

    @Test
    public void testSmallTokenRangeInMiddle() {
        LightweightOppToken token = getToken("c0ffee");
        Set<Range<LightweightOppToken>> tokenRanges = ClusterMetadataUtils.getMinimalSetOfRangesForTokens(
                        ImmutableSet.of(token), tokenRangesByStart)
                .asRanges();
        assertThat(tokenRanges).hasSize(1);
        Range<LightweightOppToken> onlyRange = tokenRanges.iterator().next();
        assertThat(onlyRange.lowerEndpoint()).isEqualTo(LightweightOppToken.serialize(SECOND_TOKEN));
        assertThat(onlyRange.upperEndpoint()).isEqualTo(token);
    }

    @Test
    public void testSmallTokenRangeAfterLastVnode() {
        LightweightOppToken token = getToken("fefe");
        Set<Range<LightweightOppToken>> tokenRanges = ClusterMetadataUtils.getMinimalSetOfRangesForTokens(
                        ImmutableSet.of(token), tokenRangesByStart)
                .asRanges();
        assertThat(tokenRanges).hasSize(1);
        Range<LightweightOppToken> onlyRange = tokenRanges.iterator().next();
        assertThat(onlyRange.lowerEndpoint()).isEqualTo(LightweightOppToken.serialize(THIRD_TOKEN));
        assertThat(onlyRange.upperEndpoint()).isEqualTo(token);
    }

    @Test
    public void testRemoveNestedRanges() {
        LightweightOppToken duplicatedStartKey = getToken("0001");
        LightweightOppToken nestedEndKey = getToken("000101");
        LightweightOppToken outerEndKey = getToken("000102");
        Range<LightweightOppToken> nested = Range.openClosed(duplicatedStartKey, nestedEndKey);
        Range<LightweightOppToken> outer = Range.openClosed(duplicatedStartKey, outerEndKey);
        assertThat(ClusterMetadataUtils.findLatestEndingRange(nested, outer)).isEqualTo(outer);
    }

    @Test
    public void testRemoveNestedWraparoundAndNonWrapRanges() {
        LightweightOppToken duplicatedStartKey = getToken("ff");
        LightweightOppToken nonWrapAround = getToken("ff01");
        Range<LightweightOppToken> nonWrapAroundRange = Range.openClosed(duplicatedStartKey, nonWrapAround);
        Range<LightweightOppToken> wrapAroundRange = Range.greaterThan(duplicatedStartKey);
        assertThat(ClusterMetadataUtils.findLatestEndingRange(nonWrapAroundRange, wrapAroundRange))
                .isEqualTo(wrapAroundRange);
    }

    private LightweightOppToken getToken(String hexBinary) {
        return LightweightOppToken.serialize(CassandraTokenRanges.getToken(hexBinary));
    }
}

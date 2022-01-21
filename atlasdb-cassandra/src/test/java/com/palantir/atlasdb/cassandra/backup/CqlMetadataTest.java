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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.CassandraTokenRanges;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.internal.core.metadata.token.DefaultTokenMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
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
        when(metadata.getTokenMap()).thenReturn(Optional.of(new DefaultTokenMap(
                ImmutableSet.of(FIRST_RANGE, SECOND_RANGE, WRAPAROUND_RANGE));

        ArgumentCaptor<ByteBuffer> componentCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
        when(metadata.newToken(componentCaptor.capture()))
                .thenAnswer(invocation -> CassandraTokenRanges.getToken(invocation.getArgument(0, ByteBuffer.class)));

        ArgumentCaptor<Token> startTokenCaptor = ArgumentCaptor.forClass(Token.class);
        ArgumentCaptor<Token> endTokenCaptor = ArgumentCaptor.forClass(Token.class);
        when(metadata.newTokenRange(startTokenCaptor.capture(), endTokenCaptor.capture()))
                .thenAnswer(this::createFromArgs);

        cqlMetadata = new CqlMetadata(metadata);
    }

    private TokenRange createFromArgs(InvocationOnMock invocation) {
        return CassandraTokenRanges.create(
                invocation.getArgument(0, Token.class), invocation.getArgument(1, Token.class));
    }

    @Test
    public void testGetTokenRanges() {
        Set<Range<LightweightOppToken>> tokenRanges = cqlMetadata.getTokenRanges();
        assertThat(tokenRanges)
                .containsExactlyInAnyOrder(
                        Range.openClosed(
                                LightweightOppToken.serialize(FIRST_TOKEN),
                                LightweightOppToken.serialize(SECOND_TOKEN)),
                        Range.openClosed(
                                LightweightOppToken.serialize(SECOND_TOKEN),
                                LightweightOppToken.serialize(THIRD_TOKEN)),
                        Range.greaterThan(LightweightOppToken.serialize(THIRD_TOKEN)),
                        Range.atMost(LightweightOppToken.serialize(FIRST_TOKEN)));

        // Turning this into a RangeSet should give the complete range (-inf, +inf)
        RangeSet<LightweightOppToken> fullTokenRing = tokenRanges.stream().collect(toImmutableRangeSet());
        assertThat(fullTokenRing.asRanges()).containsExactly(Range.all());
    }

    @Test
    public void testMinTokenGivesFullRange() {
        Token minToken = CassandraTokenRanges.minToken();
        TokenRange tokenRange = CassandraTokenRanges.create(minToken, minToken);

        Range<LightweightOppToken> fullRange =
                Iterables.getOnlyElement(cqlMetadata.makeLightweight(tokenRange).collect(Collectors.toList()));
        assertThat(fullRange).isEqualTo(Range.all());
    }

    @Test
    public void testSameTokenGivesEmptyRange() {
        TokenRange tokenRange = CassandraTokenRanges.create(FIRST_TOKEN, FIRST_TOKEN);

        Range<LightweightOppToken> emptyRange =
                Iterables.getOnlyElement(cqlMetadata.makeLightweight(tokenRange).collect(Collectors.toList()));
        assertThat(emptyRange.isEmpty()).isTrue();
    }

    @Test
    public void canGetTokenRangesByEnd() {
        Set<Range<LightweightOppToken>> tokenRanges = cqlMetadata.getTokenRanges();

        TreeMap<LightweightOppToken, Range<LightweightOppToken>> tokenRangesByEnd = KeyedStream.of(tokenRanges)
                .mapKeys(LightweightOppToken::getUpperInclusive)
                .collectTo(TreeMap::new);

        assertThat(tokenRangesByEnd).hasSize(4);
    }

    @Test
    public void testReverseConversionNoLowerBound() {
        Range<LightweightOppToken> lowerUnbounded = Range.atMost(LightweightOppToken.serialize(FIRST_TOKEN));
        TokenRange lowerTokenRange = cqlMetadata.toTokenRange(lowerUnbounded);
        assertThat(lowerTokenRange.getStart()).isEqualTo(CassandraTokenRanges.minToken());
        assertThat(lowerTokenRange.getEnd()).isEqualTo(FIRST_TOKEN);
    }

    @Test
    public void testReverseConversionNoUpperBound() {
        Range<LightweightOppToken> upperUnbounded = Range.greaterThan(LightweightOppToken.serialize(FIRST_TOKEN));
        TokenRange lowerTokenRange = cqlMetadata.toTokenRange(upperUnbounded);
        assertThat(lowerTokenRange.getStart()).isEqualTo(FIRST_TOKEN);
        assertThat(lowerTokenRange.getEnd()).isEqualTo(CassandraTokenRanges.minToken());
    }

    @Test
    public void testReverseConversionRejectsWrongBounds() {
        LightweightOppToken lowerBound = LightweightOppToken.serialize(FIRST_TOKEN);
        LightweightOppToken upperBound = LightweightOppToken.serialize(SECOND_TOKEN);

        Range<LightweightOppToken> closedOpen = Range.closedOpen(lowerBound, upperBound);
        assertThatThrownBy(() -> cqlMetadata.toTokenRange(closedOpen))
                .isExactlyInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("Token range lower bound should be open");
    }
}

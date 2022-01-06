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

import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CqlMetadata {
    private final Metadata metadata;

    public CqlMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public KeyspaceMetadata getKeyspaceMetadata(String keyspace) {
        return metadata.getKeyspace(keyspace);
    }

    // This needs to be a Set<Range>, because we don't want to merge the token ranges.
    public Set<Range<LightweightOppToken>> getTokenRanges() {
        return makeLightweight(metadata.getTokenRanges());
    }

    public LightweightOppToken newToken(ByteBuffer byteBuffer) {
        return LightweightOppToken.serialize(metadata.newToken(byteBuffer));
    }

    private Set<Range<LightweightOppToken>> makeLightweight(Set<TokenRange> tokenRanges) {
        return tokenRanges.stream().flatMap(this::makeLightweight).collect(Collectors.toSet());
    }

    @VisibleForTesting
    Stream<Range<LightweightOppToken>> makeLightweight(TokenRange tokenRange) {
        if (tokenRange.getStart().equals(minToken()) && tokenRange.getEnd().equals(minToken())) {
            // Special case - if the start and end are both minToken, then the range covers the whole ring.
            return Stream.of(Range.all());
        }

        LightweightOppToken startToken = LightweightOppToken.serialize(tokenRange.getStart());
        LightweightOppToken endToken = LightweightOppToken.serialize(tokenRange.getEnd());

        if (startToken.compareTo(endToken) <= 0) {
            return Stream.of(Range.openClosed(startToken, endToken));
        } else {
            // Handle wrap-around
            Range<LightweightOppToken> greaterThan = Range.greaterThan(startToken);
            Range<LightweightOppToken> lessThanOrEqual = Range.atMost(endToken);
            return Stream.of(greaterThan, lessThanOrEqual);
        }
    }

    public Set<Host> getReplicas(String keyspace, Range<LightweightOppToken> range) {
        return metadata.getReplicas(quotedKeyspace(keyspace), toTokenRange(range));
    }

    @VisibleForTesting
    TokenRange toTokenRange(Range<LightweightOppToken> range) {
        Preconditions.checkArgument(
                !range.hasLowerBound() || range.lowerBoundType().equals(BoundType.OPEN),
                "Token range lower bound should be open",
                SafeArg.of("range", range));
        Preconditions.checkArgument(
                !range.hasUpperBound() || range.upperBoundType().equals(BoundType.CLOSED),
                "Token range upper bound should be closed",
                SafeArg.of("range", range));

        Token lower =
                range.hasLowerBound() ? metadata.newToken(range.lowerEndpoint().deserialize()) : minToken();
        Token upper =
                range.hasUpperBound() ? metadata.newToken(range.upperEndpoint().deserialize()) : minToken();
        return metadata.newTokenRange(lower, upper);
    }

    private Token minToken() {
        return metadata.newToken(ByteBuffer.allocate(0));
    }

    private static String quotedKeyspace(String keyspaceName) {
        return "\"" + keyspaceName + "\"";
    }
}

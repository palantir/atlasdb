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

import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.stream.Stream;

public class CqlMetadata {
    private final Metadata metadata;

    public CqlMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public KeyspaceMetadata getKeyspace(String keyspace) {
        return metadata.getKeyspace(keyspace);
    }

    public RangeSet<LightweightOppToken> getTokenRanges() {
        Set<TokenRange> tokenRanges = metadata.getTokenRanges();
        return makeLightweight(tokenRanges);
    }

    private static RangeSet<LightweightOppToken> makeLightweight(Set<TokenRange> tokenRanges) {
        return tokenRanges.stream().flatMap(CqlMetadata::makeLightweight).collect(toImmutableRangeSet());
    }

    private static Stream<Range<LightweightOppToken>> makeLightweight(TokenRange tokenRange) {
        LightweightOppToken startToken = LightweightOppToken.serialize(tokenRange.getStart());
        LightweightOppToken endToken = LightweightOppToken.serialize(tokenRange.getEnd());

        if (startToken.compareTo(endToken) <= 0) {
            return Stream.of(Range.closed(startToken, endToken));
        } else {
            // Handle wrap-around
            LightweightOppToken unbounded = unboundedToken();
            Range<LightweightOppToken> greaterThan = Range.closed(startToken, unbounded);
            Range<LightweightOppToken> atMost = Range.closed(unbounded, endToken);
            return Stream.of(greaterThan, atMost);
        }
    }

    private static LightweightOppToken unboundedToken() {
        ByteBuffer minValue = ByteBuffer.allocate(0);
        return new LightweightOppToken(
                BaseEncoding.base16().decode(Bytes.toHexString(minValue).toUpperCase()));
    }

    public Set<Host> getReplicas(String keyspace, Range<LightweightOppToken> range) {
        return metadata.getReplicas(quotedKeyspace(keyspace), toTokenRange(range));
    }

    // TODO(gs): unbounded?
    private TokenRange toTokenRange(Range<LightweightOppToken> range) {
        Token lower = metadata.newToken(range.lowerEndpoint().deserialize());
        Token upper = metadata.newToken(range.upperEndpoint().deserialize());
        return metadata.newTokenRange(lower, upper);
    }

    private static String quotedKeyspace(String keyspaceName) {
        return "\"" + keyspaceName + "\"";
    }
}

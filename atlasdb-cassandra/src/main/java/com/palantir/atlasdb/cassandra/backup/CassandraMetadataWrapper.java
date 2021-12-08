/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import java.util.Set;

public class CassandraMetadataWrapper {
    private final Metadata metadata;

    public CassandraMetadataWrapper(Metadata metadata) {
        this.metadata = metadata;
    }

    public Set<Host> getReplicas(String quotedKeyspace, TokenRange range) {
        return metadata.getReplicas(quotedKeyspace, range);
    }

    public Set<TokenRange> getTokenRanges() {
        return metadata.getTokenRanges();
    }

    public TokenRange newTokenRange(Token start, Token end) {
        return metadata.newTokenRange(start, end);
    }

    // private Stream<LightweightOppTokenRange> makeLightweight(TokenRange tokenRange) {
    //     LightweightOppToken startToken = LightweightOppToken.serialise(tokenRange.getStart());
    //     LightweightOppToken endToken = LightweightOppToken.serialise(tokenRange.getEnd());
    //
    //     if (startToken.compareTo(endToken) <= 0) {
    //         return Stream.of(LightweightOppTokenRange.of(startToken, endToken));
    //     } else {
    //         // Handle wrap-around
    //         // TODO(gs): use half-empty range instead?
    //         LightweightOppToken unbounded = unboundedToken();
    //
    //         LightweightOppTokenRange greaterThan = LightweightOppTokenRange.of(startToken, unbounded);
    //         LightweightOppTokenRange atMost = LightweightOppTokenRange.of(unbounded, endToken);
    //         return Stream.of(greaterThan, atMost);
    //     }
    // }
    //
    // private LightweightOppToken unboundedToken() {
    //     ByteBuffer minValue = ByteBuffer.allocate(0);
    //     return new LightweightOppToken(
    //             BaseEncoding.base16().decode(Bytes.toHexString(minValue).toUpperCase()));
    // }
}

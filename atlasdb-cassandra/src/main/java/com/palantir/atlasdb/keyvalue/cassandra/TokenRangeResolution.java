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

import com.google.common.collect.ImmutableSet;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Set;
import org.apache.cassandra.thrift.TokenRange;
import org.immutables.value.Value;

public final class TokenRangeResolution {
    private static final SafeLogger log = SafeLoggerFactory.get(TokenRangeResolution.class);

    private TokenRangeResolution() {
        // utility
    }

    public static boolean viewsAreConsistent(Set<Set<TokenRange>> tokenRangeViews) {
        if (tokenRangeViews.size() <= 1) {
            log.trace("<= 1 distinct views of token ranges were provided, so these must have consistent endpoints.");
            return true;
        }

        long distinctIdentityAgnosticRanges = tokenRangeViews.stream()
                .map(ranges -> ranges.stream()
                        .map(IdentityAgnosticRange::fromTokenRange)
                        .collect(ImmutableSet.toImmutableSet()))
                .map(ImmutableIdentityAgnosticRanges::of)
                .distinct()
                .count();

        if (distinctIdentityAgnosticRanges != 1) {
            return false;
        }
        log.info("Although more than 1 distinct view of the token ranges were obtained, these were consistent in their"
                + " start and end tokens and in the number of nodes seen as endpoints, which we view as acceptable.");
        return true;
    }

    @Value.Immutable
    interface IdentityAgnosticRanges {
        @Value.Parameter
        ImmutableSet<IdentityAgnosticRange> identityAgnosticRanges();
    }

    @Value.Immutable
    interface IdentityAgnosticRange {
        String startToken();

        String endToken();

        int numEndpoints();

        static IdentityAgnosticRange fromTokenRange(TokenRange tokenRange) {
            return ImmutableIdentityAgnosticRange.builder()
                    .startToken(tokenRange.getStart_token())
                    .endToken(tokenRange.getEnd_token())
                    .numEndpoints(tokenRange.getEndpoints().size())
                    .build();
        }
    }
}

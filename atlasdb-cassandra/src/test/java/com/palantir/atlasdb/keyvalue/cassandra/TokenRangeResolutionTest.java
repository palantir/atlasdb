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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.cassandra.thrift.TokenRange;
import org.junit.Test;

public class TokenRangeResolutionTest {
    private static final String TOKEN_1 = "one";
    private static final String TOKEN_2 = "deux";
    private static final String TOKEN_3 = "san";
    private static final String TOKEN_4 = "quattro";
    private static final String TOKEN_5 = "fünf";

    private static final String ENDPOINT_1 = "tim burr";
    private static final String ENDPOINT_2 = "anne teak";
    private static final String ENDPOINT_3 = "matt tress";
    private static final String ENDPOINT_4 = "tom a. toh";

    @Test
    public void zeroViewsAreConsistent() {
        assertThat(TokenRangeResolution.viewsHaveConsistentTokens(ImmutableSet.of())).isTrue();
    }

    @Test
    public void completelyAgreeingViewsAreConsistent() {
        Set<Set<TokenRange>> singleNodeRingView = ImmutableSet.of(ImmutableSet.of(createRange(TOKEN_1, TOKEN_2)));
        assertThat(TokenRangeResolution.viewsHaveConsistentTokens(singleNodeRingView)).isTrue();
    }

    @Test
    public void viewsWithDifferentRangesDoNotHaveConsistentTokens() {
        Set<TokenRange> firstNodeRingView = ImmutableSet.of(createRange(TOKEN_1, TOKEN_2), createRange(TOKEN_3,
                TOKEN_4));
        Set<TokenRange> secondNodeRingView = ImmutableSet.of(createRange(TOKEN_1, TOKEN_2), createRange(TOKEN_4,
                TOKEN_5));
        assertThat(TokenRangeResolution.viewsHaveConsistentTokens(
                ImmutableSet.of(firstNodeRingView, secondNodeRingView))).isFalse();
    }

    @Test
    public void viewsWithSameSingleRangeButDifferentEndpointsAreConsistent() {
        Set<TokenRange> firstNodeRingView = ImmutableSet.of(createRangeWithEndpoint(TOKEN_1, TOKEN_2, ENDPOINT_1));
        Set<TokenRange> secondNodeRingView = ImmutableSet.of(createRangeWithEndpoint(TOKEN_1, TOKEN_2, ENDPOINT_2));
        assertThat(TokenRangeResolution.viewsHaveConsistentTokens(
                ImmutableSet.of(firstNodeRingView, secondNodeRingView))).isTrue();
    }

    @Test
    public void viewsWithMissingRangesDoNotHaveConsistentTokens() {
        Set<TokenRange> firstNodeRingView = ImmutableSet.of(createRange(TOKEN_1, TOKEN_2));
        Set<TokenRange> secondNodeRingView = ImmutableSet.of(createRange(TOKEN_1, TOKEN_2), createRange(TOKEN_2,
                TOKEN_3));
        assertThat(TokenRangeResolution.viewsHaveConsistentTokens(
                ImmutableSet.of(firstNodeRingView, secondNodeRingView))).isFalse();
    }

    @Test
    public void consistencyAcrossMultipleRanges() {
        Set<TokenRange> firstNodeRingView = ImmutableSet.of(createRangeWithEndpoint(TOKEN_1, TOKEN_2, ENDPOINT_1),
                createRangeWithEndpoint(TOKEN_3, TOKEN_4, ENDPOINT_2));
        Set<TokenRange> secondNodeRingView = ImmutableSet.of(createRangeWithEndpoint(TOKEN_1, TOKEN_2, ENDPOINT_3),
                createRangeWithEndpoint(TOKEN_3, TOKEN_4, ENDPOINT_4));
        assertThat(TokenRangeResolution.viewsHaveConsistentTokens(
                ImmutableSet.of(firstNodeRingView, secondNodeRingView))).isTrue();
    }

    @Test
    public void oneInconsistentViewSufficesForViewsToBeInconsistent() {
        Set<TokenRange> firstNodeRingView = ImmutableSet.of(createRangeWithEndpoint(TOKEN_1, TOKEN_2, ENDPOINT_1),
                createRangeWithEndpoint(TOKEN_3, TOKEN_4, ENDPOINT_2));
        Set<TokenRange> secondNodeRingView = ImmutableSet.of(createRangeWithEndpoint(TOKEN_1, TOKEN_2, ENDPOINT_3),
                createRangeWithEndpoint(TOKEN_3, TOKEN_4, ENDPOINT_4));
        Set<TokenRange> thirdNodeRingView = ImmutableSet.of(createRangeWithEndpoint(TOKEN_1, TOKEN_2, ENDPOINT_3),
                createRangeWithEndpoint(TOKEN_4, TOKEN_5, ENDPOINT_1));

        assertThat(TokenRangeResolution.viewsHaveConsistentTokens(
                ImmutableSet.of(firstNodeRingView, secondNodeRingView, thirdNodeRingView))).isFalse();
    }

    @Test
    public void multipleViewsCanAllBeConsistent() {
        Set<Set<TokenRange>> tokenRanges = ImmutableList.of(ENDPOINT_1, ENDPOINT_2, ENDPOINT_3, ENDPOINT_4)
                .stream().map(endpoint -> createRangeWithEndpoint(TOKEN_1, TOKEN_2, endpoint))
                .map(ImmutableSet::of)
                .collect(Collectors.toSet());
        assertThat(TokenRangeResolution.viewsHaveConsistentTokens(tokenRanges)).isTrue();
    }

    @Test
    public void startEndTokenOrderingIsSignificant() {
        Set<TokenRange> firstNodeRingView = ImmutableSet.of(createRange(TOKEN_1, TOKEN_2));
        Set<TokenRange> secondNodeRingView = ImmutableSet.of(createRange(TOKEN_2, TOKEN_1));
        assertThat(TokenRangeResolution.viewsHaveConsistentTokens(
                ImmutableSet.of(firstNodeRingView, secondNodeRingView))).isFalse();
    }

    private static TokenRange createRange(String startToken, String endToken) {
        return new TokenRange(startToken, endToken, ImmutableList.of());
    }

    private static TokenRange createRangeWithEndpoint(String startToken, String endToken, String endpoint) {
        return new TokenRange(startToken, endToken, ImmutableList.of(endpoint));
    }
}
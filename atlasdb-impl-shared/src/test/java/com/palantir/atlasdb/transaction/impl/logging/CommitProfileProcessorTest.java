/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.transaction.impl.logging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

public class CommitProfileProcessorTest {
    private static final long NO_OVERHEAD = 0L;

    private long totalTime;
    private long kvsWriteTime;
    private long postCommitOverhead = 0L;
    private TransactionCommitProfile profile;

    @Test
    public void keyValueServiceWriteTimeIsAccountedFor() {
        givenTotalCommitTimeIs(100L);
        givenKvsWriteTimeIs(50L);

        whenComputingDerivedMetrics();

        thenNonPutOverheadIs(50L);
        thenNonPutOverheadMillionthsIsVeryCloseTo(500_000L);
    }

    @Test
    public void commitTimeEqualsOverheadIfKvsWriteIsInstantaneous() {
        givenTotalCommitTimeIs(100L);
        givenKvsWriteTimeIs(0L);

        whenComputingDerivedMetrics();

        thenNonPutOverheadIs(100L);
        thenNonPutOverheadMillionthsIsVeryCloseTo(1_000_000L);
    }

    @Test
    public void overheadEqualsZeroIfKvsWriteTimeEqualsCommitTime() {
        givenTotalCommitTimeIs(100L);
        givenKvsWriteTimeIs(100L);

        whenComputingDerivedMetrics();

        thenNonPutOverheadIs(0L);
        thenNonPutOverheadMillionthsIsVeryCloseTo(0L);
    }

    @Test
    public void postCommitOverheadIsAccountedFor() {
        givenTotalCommitTimeIs(100L);
        givenKvsWriteTimeIs(50L);
        givenPostCommitOverheadIs(100L);

        whenComputingDerivedMetrics();

        // 50 from the commit stage + 100 post-commit overhead
        thenNonPutOverheadIs(150L);

        // 150 out of a total time of 200 for the transaction is 75 percent
        thenNonPutOverheadMillionthsIsVeryCloseTo(750_000L);
    }

    @Test
    public void postCommitOverheadIsAccountedForEvenIfOtherOverheadsNotPresent() {
        givenTotalCommitTimeIs(50L);
        givenKvsWriteTimeIs(50L);
        givenPostCommitOverheadIs(50L);

        whenComputingDerivedMetrics();

        thenNonPutOverheadIs(50L);

        // 50 out of a total time of 100 for the transaction is 50 percent
        thenNonPutOverheadMillionthsIsVeryCloseTo(500_000L);
    }

    @Test
    public void resilientToAllZeroTimings() {
        givenTotalCommitTimeIs(0L);
        givenKvsWriteTimeIs(0L);
        givenPostCommitOverheadIs(0L);

        whenComputingDerivedMetrics();

        thenNonPutOverheadIs(0L);
        thenNonPutOverheadHasAReasonableValue();
    }

    @Test
    public void resilientToVerySmallOverheads() {
        givenTotalCommitTimeIs(2_000_000L);
        givenKvsWriteTimeIs(1_999_999L);
        givenPostCommitOverheadIs(0L);

        whenComputingDerivedMetrics();

        thenNonPutOverheadIs(1L);
        thenNonPutOverheadMillionthsIsVeryCloseTo(1L);
    }

    @Test
    public void resilientToVeryLargeOverheadsDuringCommit() {
        givenTotalCommitTimeIs(2_000_000L);
        givenKvsWriteTimeIs(1L);
        givenPostCommitOverheadIs(0L);

        whenComputingDerivedMetrics();

        thenNonPutOverheadIs(1_999_999L);
        thenNonPutOverheadMillionthsIsVeryCloseTo(999_999L);
    }

    @Test
    public void resilientToVeryLargeOverheadsAfterCommit() {
        givenTotalCommitTimeIs(1L);
        givenKvsWriteTimeIs(1L);
        givenPostCommitOverheadIs(1_999_999L);

        whenComputingDerivedMetrics();

        thenNonPutOverheadIs(1_999_999L);
        thenNonPutOverheadMillionthsIsVeryCloseTo(999_999L);
    }

    @Test
    public void resilientToVerySlowCommit() {
        givenTotalCommitTimeIs(Long.MAX_VALUE / 2);
        givenKvsWriteTimeIs(7L);
        givenPostCommitOverheadIs(Long.MAX_VALUE / 2 + 1);

        whenComputingDerivedMetrics();

        thenNonPutOverheadIs(Long.MAX_VALUE - 7L);
        thenNonPutOverheadMillionthsIsVeryCloseTo(1_000_000L);
    }

    private void givenTotalCommitTimeIs(long totalTime) {
        this.totalTime = totalTime;
    }

    private void givenKvsWriteTimeIs(long kvsWriteTime) {
        this.kvsWriteTime = kvsWriteTime;
    }

    private void givenPostCommitOverheadIs(long postCommitOverhead) {
        this.postCommitOverhead = postCommitOverhead;
    }

    private void whenComputingDerivedMetrics() {
        this.profile = getProfileWithTimings(totalTime, kvsWriteTime);
    }

    private void thenNonPutOverheadIs(long expectedNonPutOverhead) {
        assertThat(getNonPutOverhead(profile, postCommitOverhead)).isEqualTo(expectedNonPutOverhead);
    }

    private void thenNonPutOverheadMillionthsIsVeryCloseTo(long expectedNonPutOverheadMillionths) {
        assertThat(getNonPutOverheadMillionths(profile, postCommitOverhead))
                .isCloseTo(expectedNonPutOverheadMillionths, within(1L));
    }

    private void thenNonPutOverheadHasAReasonableValue() {
        assertThat(getNonPutOverheadMillionths(profile, postCommitOverhead)).isBetween(0L, 1_000_000L);
    }

    private static TransactionCommitProfile getProfileWithTimings(long totalTime, long kvsWriteTime) {
        TransactionCommitProfile profile = mock(TransactionCommitProfile.class);
        when(profile.totalCommitStageMicros()).thenReturn(totalTime);
        when(profile.keyValueServiceWriteMicros()).thenReturn(kvsWriteTime);
        return profile;
    }

    private static long getNonPutOverhead(TransactionCommitProfile profile, long postCommitOverhead) {
        return CommitProfileProcessor.getNonPutOverhead(profile, postCommitOverhead);
    }

    private static long getNonPutOverheadMillionths(TransactionCommitProfile profile, long postCommitOverhead) {
        return CommitProfileProcessor.getNonPutOverheadMillionths(
                profile,
                postCommitOverhead,
                CommitProfileProcessor.getNonPutOverhead(profile, postCommitOverhead));
    }
}

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

package com.palantir.atlasdb.transaction.impl.expectations;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.palantir.atlasdb.transaction.api.expectations.ExpectationsStatistics;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableExpectationsStatistics;
import org.junit.Test;

public class ExpectationsCallbackManagerTest {
    private final ExpectationsStatistics stats = spy(
            ImmutableExpectationsStatistics.builder().transactionAgeMillis(0L).build());

    private final ExpectationsCallbackManager manager = new ExpectationsCallbackManager();

    @Test
    public void noCallbacksRegisteredMeansNoInteraction() {
        manager.runCallbacksOnce(stats);
        verifyNoInteractions(stats);
    }

    @Test
    public void oneCallbackConsumesStats() {
        manager.registerCallback(ExpectationsStatistics::readInfoByTable);
        manager.runCallbacksOnce(stats);
        verify(stats, times(1)).readInfoByTable();
        verifyNoMoreInteractions(stats);
    }

    @Test
    public void oneCallbackConsumesStatsOnceOnTwoCallbackRunCalls() {
        manager.registerCallback(ExpectationsStatistics::readInfoByTable);
        manager.runCallbacksOnce(stats);
        manager.runCallbacksOnce(stats);
        verify(stats, times(1)).readInfoByTable();
        verifyNoMoreInteractions(stats);
    }

    @Test
    public void oneCallbackRegisteredTwiceConsumesStatsTwice() {
        manager.registerCallback(ExpectationsStatistics::readInfoByTable);
        manager.registerCallback(ExpectationsStatistics::readInfoByTable);
        manager.runCallbacksOnce(stats);
        verify(stats, times(2)).readInfoByTable();
        verifyNoMoreInteractions(stats);
    }

    @Test
    public void multipleCallbacksConsumeStats() {
        manager.registerCallback(ExpectationsStatistics::transactionAgeMillis);
        manager.registerCallback(ExpectationsStatistics::readInfoByTable);
        manager.runCallbacksOnce(stats);
        verify(stats, times(1)).readInfoByTable();
        verify(stats, times(1)).transactionAgeMillis();
        verifyNoMoreInteractions(stats);
    }

    @Test
    public void multipleCallbacksConsumeStatsOnceOnTwoCallbackRunCalls() {
        manager.registerCallback(ExpectationsStatistics::transactionAgeMillis);
        manager.registerCallback(ExpectationsStatistics::readInfoByTable);
        manager.runCallbacksOnce(stats);
        manager.runCallbacksOnce(stats);
        verify(stats, times(1)).readInfoByTable();
        verify(stats, times(1)).transactionAgeMillis();
        verifyNoMoreInteractions(stats);
    }
}

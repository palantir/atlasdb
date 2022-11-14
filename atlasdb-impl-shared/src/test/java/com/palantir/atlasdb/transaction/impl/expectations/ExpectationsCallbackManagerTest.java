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

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.palantir.atlasdb.transaction.api.expectations.ExpectationsStatistics;
import java.util.function.Consumer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class ExpectationsCallbackManagerTest {
    private final ExpectationsCallbackManager manager = new ExpectationsCallbackManager();

    @Mock
    private Consumer<ExpectationsStatistics> statsCallback1;

    @Mock
    private Consumer<ExpectationsStatistics> statsCallback2;

    @Mock
    private ExpectationsStatistics stats;

    @Test
    public void statsAreNotInteractedWithAfterRunCallbackIsCalledWithNoRegisteredCallbacks() {
        manager.runCallbacksOnce(stats);
        verifyNoInteractions(stats);
    }

    @Test
    public void statsAreConsumedAfterRunCallbacks() {
        manager.registerCallback(statsCallback1);
        manager.registerCallback(statsCallback2);
        manager.registerCallback(statsCallback1);
        manager.runCallbacksOnce(stats);
        verify(statsCallback1, times(2)).accept(stats);
        verify(statsCallback2).accept(stats);
        verifyNoMoreInteractions(stats, statsCallback1, statsCallback2);
    }

    @Test
    public void runCallbackOnceIsIdempotent() {
        manager.registerCallback(statsCallback1);
        manager.registerCallback(statsCallback2);
        manager.registerCallback(statsCallback1);
        manager.runCallbacksOnce(stats);
        manager.runCallbacksOnce(stats);
        verify(statsCallback1, times(2)).accept(stats);
        verify(statsCallback2).accept(stats);
        verifyNoMoreInteractions(stats, statsCallback1, statsCallback2);
    }
}

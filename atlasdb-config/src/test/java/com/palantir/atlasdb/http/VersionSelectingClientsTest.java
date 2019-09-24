/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.http;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.stream.IntStream;

import org.junit.Test;

import com.google.common.util.concurrent.AtomicDouble;
import com.palantir.timestamp.TimestampService;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;

public class VersionSelectingClientsTest {
    private final AtomicDouble probability = new AtomicDouble(0.0);
    private final TimestampService newService = mock(TimestampService.class);
    private final TimestampService oldService = mock(TimestampService.class);
    private final TimestampService selectingService = VersionSelectingClients.createVersionSelectingClient(
            SharedTaggedMetricRegistries.getSingleton(),
            newService,
            oldService,
            probability::get,
            TimestampService.class);

    @Test
    public void probabilityZeroAlwaysGoesToOldService() {
        IntStream.range(0, 100)
                .forEach($ -> selectingService.getFreshTimestamp());
        verify(oldService, times(100)).getFreshTimestamp();
    }

    @Test
    public void respondsToChangesInProbabilityAssignment() {
        selectingService.getFreshTimestamps(1);
        verify(oldService).getFreshTimestamps(1);
        verify(newService, never()).getFreshTimestamps(1);

        probability.set(1.0);
        selectingService.getFreshTimestamps(2);
        verify(oldService, never()).getFreshTimestamps(2);
        verify(newService).getFreshTimestamps(2);
    }

    @Test
    public void drawsSeparateSamples() {
        probability.set(0.5);
        IntStream.range(0, 100)
                .forEach($ -> selectingService.getFreshTimestamp());
        verify(oldService, atLeast(10)).getFreshTimestamp();
        verify(newService, atLeast(10)).getFreshTimestamp();
    }
}

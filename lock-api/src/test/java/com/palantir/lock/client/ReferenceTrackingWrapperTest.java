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

package com.palantir.lock.client;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.util.concurrent.Futures;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class ReferenceTrackingWrapperTest {
    private final AutoCloseable closeableDelegate = mock(AutoCloseable.class);

    @Test
    public void doesNotCloseIfThereAreReferences() {
        recordReferencesAndClose(5, 4);
        verifyNoMoreInteractions(closeableDelegate);
    }

    @Test
    public void closesIfThereAreNoMoreReferences() throws Exception {
        int referenceCount = 5;
        recordReferencesAndClose(referenceCount, referenceCount);
        verify(closeableDelegate).close();
    }

    @Test
    public void closeWorksWithConcurrentCloseRequests() throws Exception {
        int referenceCount = 999;
        ReferenceTrackingWrapper<?> referenceTrackingWrapper = new ReferenceTrackingWrapper<>(closeableDelegate);
        IntStream.range(0, referenceCount).forEach(_ind -> referenceTrackingWrapper.recordReference());

        ExecutorService executor = Executors.newFixedThreadPool(50);
        List<Future<?>> futures = IntStream.range(0, referenceCount)
                .mapToObj(temp -> executor.submit(referenceTrackingWrapper::close))
                .collect(Collectors.toList());
        futures.forEach(Futures::getUnchecked);
        verify(closeableDelegate).close();
    }

    private void recordReferencesAndClose(int referenceCount, int closingCount) {
        ReferenceTrackingWrapper<AutoCloseable> referenceTrackingWrapper =
                new ReferenceTrackingWrapper<>(closeableDelegate);
        IntStream.range(0, referenceCount).forEach(_ind -> referenceTrackingWrapper.recordReference());
        IntStream.range(0, closingCount).forEach(_ind -> referenceTrackingWrapper.close());
    }
}

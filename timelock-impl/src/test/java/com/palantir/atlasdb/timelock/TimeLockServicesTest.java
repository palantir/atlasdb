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

package com.palantir.atlasdb.timelock;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.palantir.lock.CloseableLockService;
import com.palantir.lock.LockService;
import java.io.IOException;
import org.junit.Test;

public class TimeLockServicesTest {
    @Test
    public void doNotCloseRepeatedInterfaceMultipleTimes() throws IOException {
        LockService lockService = mock(LockService.class);
        AsyncTimelockService asyncTimelockService = mock(AsyncTimelockService.class);
        AsyncTimelockResource asyncTimelockResource = mock(AsyncTimelockResource.class);

        getTimeLockServices(lockService, asyncTimelockService, asyncTimelockResource)
                .close();

        verify(asyncTimelockService, times(1)).close();
    }

    @Test
    public void closesCloseableImplementationsOfNotNecessarilyCloseableInterfaces() throws IOException {
        CloseableLockService closeableLockService = mock(CloseableLockService.class);
        AsyncTimelockService asyncTimelockService = mock(AsyncTimelockService.class);
        AsyncTimelockResource asyncTimelockResource = mock(AsyncTimelockResource.class);

        getTimeLockServices(closeableLockService, asyncTimelockService, asyncTimelockResource)
                .close();

        verify(closeableLockService).close();
    }

    @Test
    public void exceptionsWhenClosingDoNotAffectOverallClosure() throws IOException {
        CloseableLockService closeableLockService = mock(CloseableLockService.class);
        AsyncTimelockService asyncTimelockService = mock(AsyncTimelockService.class);
        AsyncTimelockResource asyncTimelockResource = mock(AsyncTimelockResource.class);

        doThrow(new RuntimeException()).when(asyncTimelockService).close();
        doThrow(new RuntimeException()).when(closeableLockService).close();

        getTimeLockServices(closeableLockService, asyncTimelockService, asyncTimelockResource)
                .close();

        verify(closeableLockService).close();
        verify(asyncTimelockService).close();
    }

    private TimeLockServices getTimeLockServices(
            LockService lockService,
            AsyncTimelockService asyncTimelockService,
            AsyncTimelockResource asyncTimelockResource) {
        return TimeLockServices.create(
                asyncTimelockService, lockService, asyncTimelockService, asyncTimelockResource, asyncTimelockService);
    }
}

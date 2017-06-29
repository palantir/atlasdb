/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.lock;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.concurrent.Semaphore;

import org.junit.Test;

import com.palantir.lock.CloseableRemoteLockService;
import com.palantir.lock.impl.ThreadPooledLockService;

public class ThreadPooledLockServiceTest {

    @Test
    public void closesDelegate() throws IOException {
        CloseableRemoteLockService delegate = mock(CloseableRemoteLockService.class);
        ThreadPooledLockService pooledService = new ThreadPooledLockService(delegate, 1, new Semaphore(1));

        pooledService.close();

        verify(delegate).close();
    }

}

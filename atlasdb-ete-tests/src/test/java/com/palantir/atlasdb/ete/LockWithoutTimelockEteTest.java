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

package com.palantir.atlasdb.ete;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.palantir.atlasdb.lock.LockResource;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.v2.LockResponse;

public class LockWithoutTimelockEteTest {
    private LockResource lockResource = EteSetup.createClientToSingleNode(LockResource.class);

    @Test
    public void hugeTimelockLockSucceeds() throws InterruptedException {
        LockResponse response = lockResource.lockWithTimelock(100, 500_000);
        assertThat(response.wasSuccessful()).isTrue();
    }

    @Test
    public void hugeLockServiceLockSucceeds() throws InterruptedException {
        LockRefreshToken response = lockResource.lockWithLockService(100, 500_000);
        assertThat(response).isNotNull();
    }
}

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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.palantir.atlasdb.lock.LockResource;

public class LockWithTimelockEteTest {
    private LockResource lockResource = EteSetup.createClientToSingleNode(LockResource.class);

    @Test
    public void smallTimelockLockSucceeds() throws InterruptedException, JsonProcessingException {
        assertThat(lockResource.lockWithTimelock(1, 100)).isTrue();
    }

    @Test
    public void smallLockServiceLockSucceeds() throws InterruptedException {
        assertThat(lockResource.lockWithLockService(1, 100)).isTrue();
    }

    @Test
    public void largeTimelockLockSucceeds() throws InterruptedException, JsonProcessingException {
        assertThat(lockResource.lockWithTimelock(50, 100_000)).isTrue();
    }

    @Test
    public void largeLockServiceLockSucceeds() throws InterruptedException {
        assertThat(lockResource.lockWithLockService(50, 100_000)).isTrue();

    }

    @Test
    public void hugeTimelockLockThrowsOnClientSide() throws InterruptedException {
        assertThatThrownBy(() -> lockResource.lockWithTimelock(100, 500_000))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("INVALID_ARGUMENT");
    }

    @Test
    public void hugeLockServiceLockThrowsOnClientSide() throws InterruptedException {
        assertThatThrownBy(() -> lockResource.lockWithLockService(100, 500_000))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("INVALID_ARGUMENT");
    }
}

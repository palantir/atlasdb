/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.lock.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.client.LockRefreshingLockService;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public final class LockServiceImplTest {
    @Test
    public void verifySerializedBatchOfLockRequestsSmallerThan45MB() throws InterruptedException, IOException {
        LockService lockService = LockServiceImpl.create(
                LockServerOptions.builder().isStandaloneServer(false).build());

        Set<LockRefreshToken> tokens = new HashSet<>();

        // divide batch size by 1000 and check size in KB as approximation
        for (int i = 0; i < LockRefreshingLockService.REFRESH_BATCH_SIZE / 1000; i++) {
            LockRequest request = LockRequest.builder(ImmutableSortedMap.of(
                            StringLockDescriptor.of(UUID.randomUUID().toString()), LockMode.READ))
                    .timeoutAfter(SimpleTimeDuration.of(10, TimeUnit.MILLISECONDS))
                    .build();
            tokens.add(lockService.lock("test", request));
        }

        Assertions.assertThat(new ObjectMapper().writeValueAsString(tokens)).hasSizeLessThan(45_000);
    }
}

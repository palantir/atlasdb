/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.timelock;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.StringLockDescriptor;

public class LockServiceHealthCheck extends HealthCheck {
    private static final int OBTAIN_LOCK_TIMEOUT = 5;

    private static final LockRequest HEALTHCHECK_LOCK_REQUEST = LockRequest
            .builder(ImmutableSortedMap.of(
                    StringLockDescriptor.of("HEALTHCHECK_LOCK_REQUEST"),
                    LockMode.WRITE))
            .blockForAtMost(SimpleTimeDuration.of(OBTAIN_LOCK_TIMEOUT, TimeUnit.SECONDS))
            .timeoutAfter(SimpleTimeDuration.of(5, TimeUnit.SECONDS))
            .build();

    private final RemoteLockService service;

    public LockServiceHealthCheck(RemoteLockService service) {
        this.service = service;
    }

    @Override
    protected Result check() throws Exception {
        LockRefreshToken lockRefreshToken = service.lock("lock-service-healthcheck", HEALTHCHECK_LOCK_REQUEST);

        Preconditions.checkState(lockRefreshToken != null,
                "Could not obtain a lock in %s seconds, which indicates that the service is unhealthy.",
                OBTAIN_LOCK_TIMEOUT);

        service.unlock(lockRefreshToken);

        return Result.healthy();
    }
}

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

package com.palantir.atlasdb.backup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.timelock.api.ConjureLockImmutableTimestampResponse;
import com.palantir.atlasdb.timelock.api.ConjureLockToken;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.SuccessfulLockImmutableTimestampResponse;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.tokens.auth.AuthHeader;
import java.util.Optional;
import java.util.UUID;
import org.junit.Test;

public class AtlasBackupServiceTest {
    private final ConjureTimelockService conjureTimelockService = mock(ConjureTimelockService.class);

    // TODO(gs): replace with create method?
    private final AtlasBackupService atlasBackupService = new AtlasBackupService(conjureTimelockService);

    @Test
    public void test() {
        UUID requestId = UUID.randomUUID();
        when(conjureTimelockService.lockImmutableTimestamp(any(), any()))
                .thenReturn(ConjureLockImmutableTimestampResponse.successful(
                        SuccessfulLockImmutableTimestampResponse.of(ConjureLockToken.of(requestId), 1L)));

        AuthHeader authHeader = AuthHeader.valueOf("header");
        Optional<LockImmutableTimestampResponse> lockToken = atlasBackupService.prepareBackup(authHeader, "test");
        assertThat(lockToken).contains(LockImmutableTimestampResponse.of(1L, LockToken.of(requestId)));
    }
}

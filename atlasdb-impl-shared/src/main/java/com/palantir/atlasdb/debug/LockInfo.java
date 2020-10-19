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

package com.palantir.atlasdb.debug;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.immutables.value.Value;

/**
 * TODO(fdesouza): Remove this once PDS-95791 is resolved.
 * @deprecated Remove this once PDS-95791 is resolved.
 */
@Deprecated
@Value.Immutable
@JsonDeserialize(as = ImmutableLockInfo.class)
@JsonSerialize(as = ImmutableLockInfo.class)
public interface LockInfo {
    UUID requestId();
    Map<LockState, Instant> lockStates();

    static LockInfo newRequest(UUID requestId, Instant instant) {
        return ImmutableLockInfo.builder()
                .requestId(requestId)
                .putLockStates(LockState.ACQUIRING, instant)
                .build();
    }

    default LockInfo nextStage(LockState lockState, Instant instant) {
        return ImmutableLockInfo.builder().from(this).putLockStates(lockState, instant).build();
    }
}

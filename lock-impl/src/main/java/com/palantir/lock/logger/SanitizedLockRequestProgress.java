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

package com.palantir.lock.logger;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.lock.impl.LockServiceStateDebugger;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableSanitizedLockRequestProgress.class)
@JsonDeserialize(as = ImmutableSanitizedLockRequestProgress.class)
public interface SanitizedLockRequestProgress {
    String UNKNOWN_NEXT_LOCK_MESSAGE = "<unknown; this may have happened if the request has been serviced,"
            + " but we have not cleared it from the map>";

    List<SimpleLockRequest> getRequests();
    String getNextLock();
    int getNumLocksAcquired();
    int getTotalNumLocks();

    static SanitizedLockRequestProgress create(LockServiceStateDebugger.LockRequestProgress progress,
            LockDescriptorMapper descriptorMapper,
            String clientId) {
        return ImmutableSanitizedLockRequestProgress.builder()
                .totalNumLocks(progress.getTotalNumLocks())
                .numLocksAcquired(progress.getNumLocksAcquired())
                .nextLock(progress.getNextLock().map(descriptorMapper::getDescriptorMapping)
                        .orElse(UNKNOWN_NEXT_LOCK_MESSAGE))
                .requests(
                        StreamSupport.stream(progress.getRequest().getLockDescriptors().entries().spliterator(), false)
                                .map(descriptor -> SimpleLockRequest.of(
                                        progress.getRequest(),
                                        descriptorMapper.getDescriptorMapping(descriptor.getKey()),
                                        descriptor.getValue(),
                                        clientId))
                        .collect(Collectors.toList()))
                .build();
    }
}

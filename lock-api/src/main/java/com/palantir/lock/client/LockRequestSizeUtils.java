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

package com.palantir.lock.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.palantir.lock.v2.LockRequest;

public final class LockRequestSizeUtils {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final long PAYLOAD_LIMIT_BYTES = 50_000_000L;

    private LockRequestSizeUtils() {
        // utility
    }

    public static void validateLockRequestSize(LockRequest lockRequest) {
        validateSize(lockRequest);
    }

    public static void validateLockRequestSize(com.palantir.lock.LockRequest lockRequest) {
        validateSize(lockRequest);
    }

    private static void validateSize(Object lockRequest) {
        try {
            int sizeInBytes = OBJECT_MAPPER.writeValueAsBytes(lockRequest).length;
            Preconditions.checkArgument(sizeInBytes < PAYLOAD_LIMIT_BYTES,
                    String.format("LockRequest size too large. Size of payload: %s bytes, maximum allowed: %s bytes",
                            sizeInBytes, PAYLOAD_LIMIT_BYTES));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Error serializing the LockRequest.", e);
        }
    }
}

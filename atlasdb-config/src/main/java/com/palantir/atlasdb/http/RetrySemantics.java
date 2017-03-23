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
package com.palantir.atlasdb.http;

import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.palantir.lock.RemoteLockService;

import feign.RetryableException;

public enum RetrySemantics {
    DEFAULT (true),
    NEVER_EXCEPT_ON_NON_LEADERS (false);

    private final boolean shouldRetryHttpConnections;

    private RetrySemantics(boolean shouldRetryHttpConnections) {
        this.shouldRetryHttpConnections = shouldRetryHttpConnections;
    }

    // See internal ticket PDS-50301, and/or #1680
    @VisibleForTesting
    static final Set<Class<?>> CLASSES_TO_NOT_RETRY = ImmutableSet.of(RemoteLockService.class);

    public static <T> RetrySemantics getSemanticsFor(Class<T> clazz) {
        if (CLASSES_TO_NOT_RETRY.contains(clazz)) {
            return NEVER_EXCEPT_ON_NON_LEADERS;
        }
        return DEFAULT;
    }

    public static <T> boolean shouldRetryHttpConnections(Class<T> clazz) {
        return shouldRetryHttpConnections(getSemanticsFor(clazz));
    }

    public static boolean isFastFailoverException(RetryableException ex) {
        // If this is not-null, then we interpret this to mean that the server has thrown a 503 (so it might
        // not have been the leader).
        return ex.retryAfter() != null;
    }

    private static boolean shouldRetryHttpConnections(RetrySemantics retrySemantics) {
        return retrySemantics.shouldRetryHttpConnections;
    }
}

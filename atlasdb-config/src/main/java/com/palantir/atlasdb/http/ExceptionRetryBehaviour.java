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

import com.palantir.lock.BlockingTimeoutException;

import feign.RetryableException;

public enum ExceptionRetryBehaviour {
    RETRY_ON_OTHER_NODE(true, true),
    RETRY_INDEFINITELY_ON_SAME_NODE(true, false),
    RETRY_ON_SAME_NODE(false, false);

    private final boolean shouldRetryInfinitelyManyTimes;
    private final boolean shouldBackoffAndTryOtherNodes;

    ExceptionRetryBehaviour(boolean shouldRetryInfinitelyManyTimes, boolean shouldBackoffAndTryOtherNodes) {
        this.shouldRetryInfinitelyManyTimes = shouldRetryInfinitelyManyTimes;
        this.shouldBackoffAndTryOtherNodes = shouldBackoffAndTryOtherNodes;
    }

    public static ExceptionRetryBehaviour getRetryBehaviourForException(RetryableException retryableException) {
        if (retryableException.getCause() instanceof BlockingTimeoutException) {
            // This is the case where we have a network request that failed because it blocked too long on a lock.
            // Since it is still the leader, we want to try again on the same node.
            return RETRY_INDEFINITELY_ON_SAME_NODE;
        }

        if (retryableException.retryAfter() == null) {
            // This is the case where we have failed due to networking or other IOException.
            return RETRY_ON_OTHER_NODE;
        }

        // This is the case where the server has returned a 503.
        // This is done when we want to do fast failover because we aren't the leader or we are shutting down.
        return RETRY_ON_SAME_NODE;
    }

    public boolean shouldRetryInfinitelyManyTimes() {
        return shouldRetryInfinitelyManyTimes;
    }

    public boolean shouldBackoffAndTryOtherNodes() {
        return shouldBackoffAndTryOtherNodes;
    }
}

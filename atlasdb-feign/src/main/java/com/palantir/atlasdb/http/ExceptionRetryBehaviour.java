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
package com.palantir.atlasdb.http;

import com.palantir.atlasdb.http.errors.AtlasDbRemoteException;
import com.palantir.lock.remoting.BlockingTimeoutException;

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
        if (isCausedByBlockingTimeout(retryableException)) {
            // This is the case where we have a network request that failed because it blocked too long on a lock.
            // Since it is still the leader, we want to try again on the same node.
            return RETRY_INDEFINITELY_ON_SAME_NODE;
        }

        if (retryableException.retryAfter() != null) {
            // This is the case where the server has returned a 503.
            // This is done when we want to do fast failover because we aren't the leader or we are shutting down.
            return RETRY_ON_OTHER_NODE;
        }

        // This is the case where we have failed due to networking or other IOException.
        return RETRY_ON_SAME_NODE;
    }

    /**
     * Returns true if and only if clients which have defined a finite limit for the number
     * of retries should retry infinitely many times. Typically, this implies that the failure of the
     * node in question is not reflective of a failing condition of the cluster in general (such as the node
     * in question shutting down, or it not being the leader.)
     */
    public boolean shouldRetryInfinitelyManyTimes() {
        return shouldRetryInfinitelyManyTimes;
    }

    /**
     * Returns true if clients should, where possible, retry on other nodes before retrying on this
     * node. Note that a value of false here does not necessarily mean that clients should not retry on other nodes.
     */
    public boolean shouldBackoffAndTryOtherNodes() {
        return shouldBackoffAndTryOtherNodes;
    }

    private static boolean isCausedByBlockingTimeout(RetryableException retryableException) {
        return retryableException.getCause() instanceof AtlasDbRemoteException
                && getCausingErrorName(retryableException).equals(BlockingTimeoutException.class.getName());
    }

    private static String getCausingErrorName(RetryableException retryableException) {
        return ((AtlasDbRemoteException) retryableException.getCause()).getErrorName();
    }
}

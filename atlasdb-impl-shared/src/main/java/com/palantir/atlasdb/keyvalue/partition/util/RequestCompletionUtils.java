/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.partition.util;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.partition.exception.ClientVersionTooOldException;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumTracker;
import com.palantir.atlasdb.keyvalue.partition.util.EndpointRequestExecutor.EndpointRequestCompletionService;
import com.palantir.common.base.Throwables;

public class RequestCompletionUtils {

    private static final Logger log = LoggerFactory.getLogger(RequestCompletionUtils.class);

    // These exceptions should be thrown immediately
    private static boolean isNonInterceptableException(Throwable e) {
        return e instanceof ClientVersionTooOldException || e instanceof KeyAlreadyExistsException;
    }

    /**
     * This will block until success or failure of the request can be concluded.
     * In case of failure it will rethrow the last encountered exception.
     *
     * TODO: Is it ok if the remaining futures are not beign taken after tracker
     * is finished? Zombie threads etc.
     *
     * @param tracker
     * @param execSvc
     * @param mergeFunction
     */
    private static <TrackingUnit, FutureReturnType> void completeRequest(
            QuorumTracker<FutureReturnType, TrackingUnit> tracker,
            EndpointRequestCompletionService<FutureReturnType> execSvc,
            Function<FutureReturnType, Void> mergeFunction) {

        try {
            // Wait until we can conclude success or failure
            while (!tracker.finished()) {
                Future<FutureReturnType> future = execSvc.take();
                try {
                    FutureReturnType result = future.get();
                    mergeFunction.apply(result);
                    tracker.handleSuccess(future);
                } catch (ExecutionException e) {
                    tracker.handleFailure(future);
                    // Check if the failure is fatal
                    Throwable cause = e.getCause();
                    if (isNonInterceptableException(cause) || tracker.failed()) {
                        Throwables.rewrapAndThrowUncheckedException(cause);
                    }
                }
            }
        } catch (InterruptedException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    /**
     * In case of read request we can cancel all remaining threads as soon as completeRequests
     * returns - it means that either success or failure has been concluded.
     *
     * @param tracker
     * @param execSvc
     * @param mergeFunction
     */
    public static <TrackingUnit, FutureReturnType> void completeReadRequest(
            QuorumTracker<FutureReturnType, TrackingUnit> tracker,
            EndpointRequestCompletionService<FutureReturnType> execSvc,
            Function<FutureReturnType, Void> mergeFunction) {

        try {
            completeRequest(tracker, execSvc, mergeFunction);
        } finally {
            tracker.cancel(true);
        }
    }

    /**
     * In case of write requests we should only cancel all the threads if a failure can be
     * concluded.
     * Otherwise we just return as soon as success is concluded but we leave other write
     * tasks running in the background.
     *
     * @param tracker
     * @param execSvc
     */
    public static <TrackingUnit> void completeWriteRequest(
            final QuorumTracker<Void, TrackingUnit> tracker,
            final EndpointRequestCompletionService<Void> execSvc) {

        try {
            completeRequest(tracker, execSvc, Functions.<Void> identity());
        } catch (RuntimeException e) {
            tracker.cancel(true);
            throw e;
        }
    }

    /**
     * Keep applying the function <code>fun</code> to items retrieved from <code>iterator</code> until no
     * exception is thrown. Return the result if <code>fun</code>.
     *
     * @param iterator
     * @param fun
     * @return
     */
    public static <T, U, V extends Iterator<? extends U>> T retryUntilSuccess(V iterator, Function<U, T> fun) {

        Preconditions.checkArgument(iterator.hasNext());

        while (iterator.hasNext()) {
            U service = iterator.next();
            try {
                return fun.apply(service);
            } catch (RuntimeException e) {
                log.warn("retryUntilSuccess: " + e.getMessage());

                // These two exceptions should be thrown immediately
                if (isNonInterceptableException(e)) {
                    // Do NOT add a message here
                    Throwables.rewrapAndThrowUncheckedException(e);
                }

                if (!iterator.hasNext()) {
                    Throwables.rewrapAndThrowUncheckedException("retryUntilSuccess", e);
                }
            }
        }

        throw new RuntimeException("This should never happen!");
    }

}

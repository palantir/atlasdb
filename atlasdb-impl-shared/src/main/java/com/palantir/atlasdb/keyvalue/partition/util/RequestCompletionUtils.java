package com.palantir.atlasdb.keyvalue.partition.util;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.partition.exception.VersionTooOldException;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumTracker;
import com.palantir.common.base.Throwables;

public class RequestCompletionUtils {

    private static final Logger log = LoggerFactory.getLogger(RequestCompletionUtils.class);

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
            ExecutorCompletionService<FutureReturnType> execSvc,
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
                    Throwable cause = e.getCause();
                    // These two exceptions should be thrown immediately
                    if (cause instanceof KeyAlreadyExistsException || cause instanceof VersionTooOldException) {
                        Throwables.throwUncheckedException(cause);
                    }
                    tracker.handleFailure(future);
                    // Check if the failure is fatal
                    if (tracker.failed()) {
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
            ExecutorCompletionService<FutureReturnType> execSvc,
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
            QuorumTracker<Void, TrackingUnit> tracker,
            ExecutorCompletionService<Void> execSvc) {

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
                if (e instanceof KeyAlreadyExistsException || e instanceof VersionTooOldException) {
                    Throwables.throwUncheckedException(e);
                }

                if (!iterator.hasNext()) {
                    Throwables.rewrapAndThrowUncheckedException("retryUntilSuccess", e);
                }
            }
        }

        throw new RuntimeException("This should never happen!");
    }

}

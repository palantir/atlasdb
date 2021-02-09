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

package com.palantir.leader.proxy;

import com.palantir.common.concurrent.CoalescingSupplier;
import com.palantir.leader.LeaderElectionService.LeadershipToken;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.logsafe.SafeArg;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeadershipStateManager<T> {
    private static final Logger log = LoggerFactory.getLogger(LeadershipStateManager.class);

    private final CoalescingSupplier<LeadershipToken> leadershipTokenCoalescingSupplier;
    private final LeadershipCoordinator leadershipCoordinator;
    private final AtomicReference<LeadershipToken> maybeValidLeadershipTokenRef;
    private final Supplier<T> delegateSupplier;

    private volatile T delegateRef;
    private volatile boolean isClosed;

    public LeadershipStateManager(LeadershipCoordinator leadershipCoordinator, Supplier<T> delegateSupplier) {
        this.leadershipTokenCoalescingSupplier = new CoalescingSupplier<>(this::getOrUpdateLeadershipToken);
        this.leadershipCoordinator = leadershipCoordinator;
        this.delegateSupplier = delegateSupplier;
        this.maybeValidLeadershipTokenRef = new AtomicReference<>();
        this.isClosed = false;
    }

    LeadershipState<T> getLeadershipState() {
        return ImmutableLeadershipState.<T>builder()
                .leadershipToken(leadershipTokenCoalescingSupplier.get())
                .delegate(delegateRef)
                .build();
    }

    void close() {
        isClosed = true;
        clearDelegate();
    }

    /**
     * Checks the local ref of leadership token. We need to refresh the delegateRef in case the token has
     * changed.
     * This code can be accessed by multiple threads. In order to save all of the requests from locking-unlocking
     * `this` while trying to update maybeValidLeadershipTokenRef, the invocations are batched using CoalescingSupplier.
     */
    private LeadershipToken getOrUpdateLeadershipToken() {
        if (!leadershipCoordinator.isStillCurrentToken(maybeValidLeadershipTokenRef.get())) {
            // we need to clear out existing resources if leadership token has been updated
            releaseOldResourcesOnLeadershipUpdate();
            tryToUpdateLeadershipToken();
        }

        LeadershipToken leadershipToken = maybeValidLeadershipTokenRef.get();
        if (leadershipToken == null) {
            // reclaim delegate resources if leadership cannot be gained
            clearDelegate();
            throw leadershipCoordinator.notCurrentLeaderException("method invoked on a non-leader");
        }
        return leadershipToken;
    }

    /**
     * This method refreshes the delegateRef which can be a very expensive operation. This should be executed exactly
     * once for one leadershipToken update.
     * @throws NotCurrentLeaderException if we do not have leadership anymore.
     */
    private void tryToUpdateLeadershipToken() {
        // throws NotCurrentLeaderException.
        LeadershipToken leadershipToken = leadershipCoordinator.getLeadershipToken();

        log.debug("Getting delegate to start serving calls.");
        T delegate = null;
        try {
            delegate = delegateSupplier.get();
        } catch (Throwable t) {
            log.error("problem creating delegate", t);
        }

        if (delegate != null) {
            // Do not modify, hide, or remove this line without considering impact on correctness.
            delegateRef = delegate;
            if (isClosed) {
                clearDelegate();
            } else {
                maybeValidLeadershipTokenRef.set(leadershipToken);
                log.info("Gained leadership for {}", SafeArg.of("leadershipToken", leadershipToken));
            }
        }
    }

    private void releaseOldResourcesOnLeadershipUpdate() {
        maybeValidLeadershipTokenRef.set(null);
        clearDelegate();
    }

    private void clearDelegate() {
        Object delegate = delegateRef;
        delegateRef = null;
        if (delegate instanceof Closeable) {
            try {
                ((Closeable) delegate).close();
            } catch (IOException ex) {
                log.warn("problem closing delegate", ex);
            }
        }
    }

    /**
     * Right now there is no way to release resources quickly. In the event of loss of leadership, we wait till the
     * LeadershipCoordinator has updated its state. Then, the next request can release the delegateRef in
     * `getOrUpdateLeadershipToken`.
     */
    void handleNotLeading(final LeadershipToken leadershipToken, @Nullable Throwable cause) {
        if (maybeValidLeadershipTokenRef.compareAndSet(leadershipToken, null)) {
            // We are not clearing delegateTokenRef here. This is fine as we are relying on `getOrUpdateLeadershipToken`
            // to claim the resources for the next request if this node loses leadership.
            // If this node gains leadership again (i.e. with a different leadership token),
            // `tryToUpdateLeadershipToken` guarantees that the delegate will be refreshed *before* we get a new
            // leadershipToken.
            // If we were to clearDelegateRef here or outside the CAS, we could race with
            // `tryToUpdateLeadershipToken` and end up clearing `delegateRef`.

            leadershipCoordinator.markAsNotLeading(leadershipToken, cause);
        }
        throw leadershipCoordinator.notCurrentLeaderException(
                "method invoked on a non-leader (leadership lost)", cause);
    }

    @Value.Immutable
    interface LeadershipState<T> {
        LeadershipToken leadershipToken();

        T delegate();
    }
}

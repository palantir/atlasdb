/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.paxos;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.palantir.atlasdb.timelock.paxos.api.NamespaceLeadershipTakeoverService;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.paxos.Client;
import com.palantir.tokens.auth.AuthHeader;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class NamespaceTakeoverResource implements NamespaceLeadershipTakeoverService {

    private static final SafeLogger log = SafeLoggerFactory.get(NamespaceTakeoverResource.class);
    private static final Duration RANDOM_WAIT_BEFORE_BACKOFF = Duration.ofMillis(500);

    private final LeadershipComponents leadershipComponents;
    private final Retryer<Boolean> takeoverRetryer;

    public NamespaceTakeoverResource(LeadershipComponents leadershipComponents) {
        this.leadershipComponents = leadershipComponents;
        this.takeoverRetryer = new Retryer<>(
                StopStrategies.stopAfterAttempt(5),
                WaitStrategies.randomWait(RANDOM_WAIT_BEFORE_BACKOFF.toMillis(), TimeUnit.MILLISECONDS),
                attempt -> !attempt.hasResult() || !attempt.getResult());
    }

    @Override
    public boolean takeover(AuthHeader _authHeader, String namespace) {
        try {
            return takeoverRetryer.call(() -> nonRetriedTakeover(namespace));
        } catch (ExecutionException e) {
            log.info("request failed, should not reach here", e);
            return false;
        } catch (RetryException e) {
            log.info("failed repeatedly", SafeArg.of("numberOfAttempts", e.getNumberOfFailedAttempts()), e);
            return false;
        }
    }

    @Override
    public Set<String> takeoverNamespaces(AuthHeader _authHeader, Set<String> namespaces) {
        return namespaces.stream()
                .filter(namespace -> leadershipComponents.requestHostileTakeover(Client.of(namespace)))
                .collect(Collectors.toSet());
    }

    private boolean nonRetriedTakeover(String namespace) {
        return leadershipComponents.requestHostileTakeover(Client.of(namespace));
    }
}
